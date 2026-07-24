"""The learned half of the run estimate: a small network that corrects the
arithmetic model in workflow/helpers/run_estimates.py.

WHY A NETWORK CORRECTS THE MODEL INSTEAD OF REPLACING IT

The shape of a run is not a pattern waiting to be discovered. ceil(N/in_flight)
is what a pool of twelve slots *does*, and dividing core-seconds by cores is what
cores are for. A network asked to rediscover that from a few dozen runs would
learn it worse than it is already known, and would have to relearn it from
scratch the day the box gets more cores -- a run size and a core count it has
never seen are exactly where it would be free to say anything at all.

What is genuinely unknown is how wrong that arithmetic is *here*: BV-BRC's queue
on this account, this box's real throughput under its own concurrency, the slack
in REMOTE_SECONDS. That is a correction, it is small, and it is what this learns.

  estimate = baseline_seconds(...) x correction(features)

So the network never extrapolates the thing it would be bad at. Ask it about a
100-sample run and the arithmetic still carries the scaling; the network only
says whether runs on this instance tend to come in over or under.

WHAT IT LEARNS ON, AND WHY IT USUALLY DOES NOT

It trains on the ratios in the run history -- the same numbers ``calibration()``
takes a median of. Which is to say: a handful of points, in a space where most of
the variance is not ours to predict. On a measured run, the two BV-BRC stages
were 94% of the wall clock (assembly 1571s, genus 367s, of 2061s). That is time
spent in another organisation's queue. No architecture recovers it from
(samples, cores, in_flight), because it is not a function of them.

A model trained on four runs of a mostly-exogenous target does not estimate; it
memorises. So until MIN_TRAIN_RUNS runs have finished here, this defers to the
median and the estimate is exactly what it was before. The network earns its way
in; it does not arrive assuming it is right.

Three things keep it honest even then:

  it predicts in log space   a correction is multiplicative -- 2x too long and
                             half as long are the same size of mistake, and the
                             output cannot come out negative.
  it is centred on the mean  the network predicts the *residual* around the mean
                             log-ratio, and weight decay pulls it towards zero.
                             With nothing to say it says nothing, and the
                             estimate falls back to roughly the median anyway.
  its output is clamped      CORRECTION_LIMITS. Whatever it has convinced itself
                             of, it may not quote a run at a quarter or quadruple
                             what the arithmetic says. A bad fit degrades the
                             estimate; it cannot make it absurd.

DETERMINISM IS A CORRECTNESS PROPERTY HERE

Gunicorn runs several workers, each with its own copy of this module, and the
status page polls whichever one answers. If two workers trained on the same
history and disagreed, a countdown would jump every few seconds as polls landed
on different workers -- and the user would be watching the estimate, not the run.

So training is deterministic: seeded init, full-batch gradient descent, a fixed
epoch count, no shuffling. Same history in, same weights out, in every worker.
Do not introduce dropout, minibatch sampling, or an unseeded RNG here without
making the result a function of the history alone.

It is still an estimate, and the caller must still present it as one.
"""

import math

import numpy as np


# Runs that must have finished on this instance before the network is allowed to
# say anything. Below this it defers to the median (run_estimates.calibration).
#
# Not a tuning knob so much as an honesty threshold. The history holds at most
# HISTORY_LIMIT (25) runs and the target is mostly BV-BRC's queue, so a fit on
# three or four points is memorisation wearing a network's clothes. Ten is enough
# that a single freak run cannot carry the fit, and still leaves room inside the
# history for the model to keep moving as the instance changes.
MIN_TRAIN_RUNS = 10

# How far the network may move an estimate off the arithmetic. The model's shape
# is trusted; only its scale is in question, and a scale error of more than 4x is
# not a calibration -- it is the arithmetic being wrong, which is a thing to go
# fix in run_estimates.py rather than to paper over here.
CORRECTION_LIMITS = (0.25, 4.0)

# Small on purpose. There are at most 25 training points and five features, and
# the function being learned is a gentle one -- "runs here tend to come in a bit
# under, more so when the box is busy". Capacity beyond this buys nothing but the
# opportunity to fit noise.
HIDDEN_UNITS = 8
EPOCHS = 400
LEARNING_RATE = 0.05
WEIGHT_DECAY = 0.01
SEED = 20260714

# Bumped when the features or the architecture change, so that a cached net is
# never reused across a change in what it means. (The stored *ratios* are not
# versioned by this -- they are a property of the arithmetic model, not of this
# network, and survive a change here untouched.)
NET_VERSION = 1


def features(samples, assemblies, bvbrc_in_flight, cores, rounds):
	"""What the network gets to look at.

	Every one of these is known before a run starts -- that is the whole
	constraint on this list, since the estimate is quoted to a job that has not
	started yet.

	The log terms are there because the quantities act multiplicatively: the
	difference between 1 and 2 samples is the difference between 24 and 48, and a
	raw count would make the network learn that twice over at two different
	scales.

	``rounds`` is passed in rather than recomputed so that this cannot drift from
	the arithmetic model's own idea of a round."""
	samples = max(0, int(samples))
	assemblies = max(0, int(assemblies))
	return np.array(
		[
			math.log1p(samples),
			math.log1p(assemblies),
			# How much of the run is remote work. A re-run holding all its
			# assemblies is a different animal from a cold run of the same size,
			# and this is the term that lets the network say so.
			(assemblies / samples) if samples else 0.0,
			math.log1p(max(1, cores)),
			math.log1p(max(1, rounds)),
		],
		dtype=np.float64,
	)


def _standardise(feature_matrix):
	"""Centre and scale, with a floor on the spread.

	The floor matters more than it looks: a history in which every run had the
	same core count -- the normal case for one box -- gives that column zero
	variance, and dividing by it would be a divide-by-zero rather than the
	"ignore this column" it should be."""
	mean = feature_matrix.mean(axis=0)
	spread = feature_matrix.std(axis=0)
	spread[spread < 1e-6] = 1.0
	return mean, spread


class _Net:
	"""One hidden layer, tanh, scalar out. Adam, by hand, because this is five
	matrices and reaching for a deep-learning runtime to hold them would cost
	about a gigabyte of image for a few hundred parameters."""

	def __init__(self, feature_count, rng):
		# Xavier-ish: keeps the initial pre-activations inside tanh's linear
		# region, so early gradients are neither saturated nor exploding.
		self.hidden_weights = rng.normal(0, 1 / math.sqrt(feature_count), (feature_count, HIDDEN_UNITS))
		self.hidden_bias = np.zeros(HIDDEN_UNITS)
		# Zero, deliberately: an untrained net outputs exactly 0, and 0 is the
		# residual that means "the mean log-ratio, unmodified". The network starts
		# life agreeing with the median and has to be argued out of it.
		self.output_weights = np.zeros((HIDDEN_UNITS, 1))
		self.output_bias = np.zeros(1)

	def forward(self, inputs):
		self._inputs = inputs
		self._hidden = np.tanh(inputs @ self.hidden_weights + self.hidden_bias)
		return self._hidden @ self.output_weights + self.output_bias

	def backward(self, output_gradient):
		batch = self._inputs.shape[0]
		grad_output_weights = self._hidden.T @ output_gradient / batch
		grad_output_bias = output_gradient.mean(axis=0)
		hidden_gradient = (output_gradient @ self.output_weights.T) * (1 - self._hidden**2)
		grad_hidden_weights = self._inputs.T @ hidden_gradient / batch
		grad_hidden_bias = hidden_gradient.mean(axis=0)
		return [grad_hidden_weights, grad_hidden_bias, grad_output_weights, grad_output_bias]

	def parameters(self):
		return [self.hidden_weights, self.hidden_bias, self.output_weights, self.output_bias]


def _train(feature_matrix, targets):
	"""Full-batch Adam on the centred log-ratios. Deterministic: see the module
	docstring -- two gunicorn workers must land on identical weights."""
	rng = np.random.default_rng(SEED)
	net = _Net(feature_matrix.shape[1], rng)
	parameters = net.parameters()
	first_moment = [np.zeros_like(parameter) for parameter in parameters]
	second_moment = [np.zeros_like(parameter) for parameter in parameters]

	for step in range(1, EPOCHS + 1):
		predictions = net.forward(feature_matrix)
		# Mean squared error on the residual, d/dp of (p - t)^2.
		gradients = net.backward(2.0 * (predictions - targets))
		for index, (parameter, gradient) in enumerate(zip(parameters, gradients)):
			# Decay the weights, not the biases: shrinking a bias would pull the
			# net away from the mean it is supposed to default to.
			if parameter.ndim > 1:
				gradient = gradient + WEIGHT_DECAY * parameter
			first_moment[index] = 0.9 * first_moment[index] + 0.1 * gradient
			second_moment[index] = 0.999 * second_moment[index] + 0.001 * gradient**2
			corrected_first = first_moment[index] / (1 - 0.9**step)
			corrected_second = second_moment[index] / (1 - 0.999**step)
			parameter -= LEARNING_RATE * corrected_first / (np.sqrt(corrected_second) + 1e-8)
	return net


class CorrectionModel:
	"""A trained net plus everything needed to feed it: the normalisation it was
	fitted with, and the mean log-ratio its output is a residual around.

	Built by ``train_from_history``; ``correction`` is the only thing callers
	want from it."""

	def __init__(self, net, feature_mean, feature_spread, mean_log_ratio, run_count):
		self.net = net
		self.feature_mean = feature_mean
		self.feature_spread = feature_spread
		self.mean_log_ratio = mean_log_ratio
		self.run_count = run_count

	def correction(self, samples, assemblies, bvbrc_in_flight, cores, rounds):
		"""The multiplier to put on ``baseline_seconds``. Always inside
		CORRECTION_LIMITS, whatever the network thinks."""
		raw = features(samples, assemblies, bvbrc_in_flight, cores, rounds)
		normalised = ((raw - self.feature_mean) / self.feature_spread).reshape(1, -1)
		residual = float(self.net.forward(normalised)[0, 0])
		correction = math.exp(self.mean_log_ratio + residual)
		lower, upper = CORRECTION_LIMITS
		return min(max(correction, lower), upper)


def train_from_history(history, rounds_for):
	"""Fit a correction model to the run history, or return None to defer.

	None is the honest answer far more often than not, and callers must treat it
	as "use the median" rather than as a failure. It comes back when there are
	fewer than MIN_TRAIN_RUNS runs to learn from -- see the module docstring on
	why a fit on four points is not an estimate.

	``rounds_for(entry)`` is supplied by the caller so that this module does not
	have to import the arithmetic model it corrects (and so the two cannot
	disagree about what a round is)."""
	usable = [
		entry
		for entry in history
		if isinstance(entry.get("factor"), (int, float)) and entry["factor"] > 0
	]
	if len(usable) < MIN_TRAIN_RUNS:
		return None

	feature_matrix = np.array(
		[
			features(
				entry.get("samples", 0),
				entry.get("assemblies", entry.get("samples", 0)),
				entry.get("bvbrc_in_flight", 1),
				entry.get("cores", 1),
				rounds_for(entry),
			)
			for entry in usable
		]
	)
	log_ratios = np.array([math.log(entry["factor"]) for entry in usable]).reshape(-1, 1)

	feature_mean, feature_spread = _standardise(feature_matrix)
	normalised = (feature_matrix - feature_mean) / feature_spread

	# The network learns the residual around the mean, so that its zero-output
	# initial state is already the "no opinion" answer.
	mean_log_ratio = float(log_ratios.mean())
	net = _train(normalised, log_ratios - mean_log_ratio)

	return CorrectionModel(net, feature_mean, feature_spread, mean_log_ratio, len(usable))
