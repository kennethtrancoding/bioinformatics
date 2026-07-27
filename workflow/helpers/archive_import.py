"""Expanding an uploaded .zip into a staging tree, safely.

The sequencing company's run folder often arrives as one .zip rather than as a
folder, and re-expanding it by hand only to upload the contents is the slowest
possible way to move 20 GB. So the upload accepts the archive itself and this
module opens it.

An uploaded archive is untrusted input, and the standard library will happily do
what it is told with one: ``ZipFile.extractall`` writes wherever the member names
say, including outside the directory it was given (a member called
``../../etc/cron.d/x``), and it will keep writing however many bytes the stream
produces, whatever the header claimed. This module refuses those instead:

  * **Escapes.** ``..``, absolute names and Windows drive letters are refused
    outright, and every component is put through ``secure_filename`` before the
    final path is checked, again, to be inside the destination.
  * **Links and devices.** A symlink member is the other way out of the
    destination -- extract ``reads -> /`` and the next member writes through it --
    so any member that is not a regular file is refused.
  * **Bombs.** Member count, per-member size, total size and compression ratio are
    all capped, the destination's free disk is checked against the declared total
    before anything is written, and each member is streamed with a hard ceiling so
    a lying header is caught while it is being extracted rather than after the
    disk has filled.
  * **Anything we would not have imported anyway.** Only the file types the
    importer reads (FASTQ, the stats workbook, checksum sidecars) are written at
    all. A nested archive, a script or an executable is left in the zip -- the
    quietest way to keep a mislabelled archive from turning into a delivery
    mechanism, and it costs nothing, because the importer would ignore those files.

The limits below are deliberately generous -- a genuine run folder is enormous and
this must not refuse real data -- but they are finite, which is the point.
Every one can be overridden with an environment variable for an unusual delivery.

Nothing here trusts the archive's own account of itself: sizes are enforced
against the bytes that actually arrive.
"""

import os
import shutil
import stat
import zipfile
from pathlib import Path

from werkzeug.utils import secure_filename

from workflow.helpers.import_samples import is_fastq


class UnsafeArchiveError(Exception):
	"""The archive is malformed or hostile. Nothing from it is kept."""


def _limit(variable_name, default):
	"""An override from the environment, or the default. Blank reads as unset, so
	a systemd unit that declares the variable without a value does not crash on
	int('')."""
	return int(os.environ.get(variable_name) or default)


GIB = 1024**3

# Files in one archive. A 200-sample run folder is ~400 FASTQs plus sidecars.
MAX_MEMBERS = _limit("ZIP_MAX_MEMBERS", 20_000)
# One file's uncompressed size. A deep-sequenced read file is a few GB.
MAX_MEMBER_BYTES = _limit("ZIP_MAX_MEMBER_BYTES", 64 * GIB)
# Everything in the archive, uncompressed.
MAX_TOTAL_BYTES = _limit("ZIP_MAX_TOTAL_BYTES", 512 * GIB)
# Uncompressed / compressed. FASTQs arrive gzipped inside the zip (ratio ~1);
# even plain FASTQ text only deflates 4-6x. A classic zip bomb is 1000x and up.
MAX_COMPRESSION_RATIO = _limit("ZIP_MAX_COMPRESSION_RATIO", 200)
# Below this a high ratio means nothing -- a few hundred bytes of repeated text
# legitimately compresses to almost nothing -- so the ratio is only judged on
# members big enough for it to be evidence of anything.
RATIO_FLOOR_BYTES = 4096
# Leave the box this much room after the archive is expanded.
FREE_DISK_HEADROOM_BYTES = _limit("ZIP_FREE_DISK_HEADROOM_BYTES", GIB)

# What the importer actually reads: reads, the sequencing company's stats
# workbook, and the checksum/manifest sidecars that sometimes come with it.
IMPORTABLE_SUFFIXES = (".xlsx", ".csv", ".tsv", ".txt", ".md5")

_ZIP_SUFFIX = ".zip"
_ENCRYPTED_FLAG = 0x1


def is_zip_name(file_name):
	"""Whether a filename names a zip archive. The single definition, mirrored by
	ZIP_SUFFIX in static/app.js."""
	return (file_name or "").lower().endswith(_ZIP_SUFFIX)


def _is_importable(file_name):
	return is_fastq(file_name) or file_name.lower().endswith(IMPORTABLE_SUFFIXES)


def _safe_relative_path(member_name):
	"""The path a member may be written to, relative to the destination.

	Raises UnsafeArchiveError if the name tries to leave the destination. A
	traversal attempt is not a file to skip quietly: nothing legitimate produces
	one, so the archive it came in is not one to import half of.
	"""
	normalized_name = (member_name or "").replace("\\", "/")
	if normalized_name.startswith("/") or ":" in normalized_name.split("/")[0]:
		raise UnsafeArchiveError(f"Archive member has an absolute path: {member_name!r}")
	parts = []
	for name_part in normalized_name.split("/"):
		if name_part in ("", "."):
			continue
		if name_part == "..":
			raise UnsafeArchiveError(
				f"Archive member tries to escape the upload folder: {member_name!r}"
			)
		safe_part = secure_filename(name_part)
		if safe_part:
			parts.append(safe_part)
	return Path(*parts) if parts else None


def _reject_non_regular(member):
	"""Refuse symlinks, devices and anything else that is not a plain file.

	The high 16 bits of external_attr carry the Unix mode for archives that
	recorded one. Only the file-type half of it is judged, and only when it is
	actually set: plenty of ordinary archives (anything written with
	ZipFile.writestr, for one) store permission bits and no type at all, and that
	is not a claim to be a symlink.
	"""
	file_type = stat.S_IFMT(member.external_attr >> 16)
	if file_type not in (0, stat.S_IFREG):
		kind = "symlink" if file_type == stat.S_IFLNK else "special file"
		raise UnsafeArchiveError(f"Archive contains a {kind} ({member.filename!r}); refused.")


def _copy_member(archive, member, destination_path):
	"""Stream one member to disk, stopping the moment it exceeds what it declared.

	The declared size is what every check above was made against, so it is also
	the ceiling enforced here -- a member that keeps producing bytes past it has
	lied about itself, and the extraction is over. Returns bytes written.
	"""
	ceiling = min(member.file_size, MAX_MEMBER_BYTES)
	written = 0
	with archive.open(member) as member_stream, destination_path.open("wb") as destination_file:
		while True:
			chunk = member_stream.read(1 << 20)
			if not chunk:
				break
			written += len(chunk)
			if written > ceiling:
				raise UnsafeArchiveError(
					f"Archive member {member.filename!r} is larger than the "
					f"{member.file_size} bytes it declared; refused."
				)
			destination_file.write(chunk)
	return written


def extract_zip(archive_source, dest_dir, warnings=None, display_name=None):
	"""Expand an archive into `dest_dir`, keeping only importable files.

	`archive_source` is a path, or any seekable binary file object -- an upload's
	own stream, so a 20 GB zip that Werkzeug has already buffered to disk is read
	where it lies instead of being copied first.

	`dest_dir` belongs to this archive alone: it is created here and removed
	again if anything in the archive turns out to be unsafe, so a refused archive
	never leaves half of itself behind for the importer to find.

	Appends a line to `warnings` for anything skipped. Returns the number of
	files written. Raises UnsafeArchiveError if the archive is unreadable or
	hostile, or if the box has no room for it.
	"""
	warnings = warnings if warnings is not None else []
	dest_dir = Path(dest_dir)
	is_path = isinstance(archive_source, (str, Path))
	zip_name = display_name or (Path(archive_source).name if is_path else "the archive")

	if not is_path:
		archive_source.seek(0)
	if not zipfile.is_zipfile(archive_source):
		raise UnsafeArchiveError(f"{zip_name} is not a readable .zip archive.")
	if not is_path:
		archive_source.seek(0)

	try:
		with zipfile.ZipFile(archive_source) as archive:
			members = [member for member in archive.infolist() if not member.is_dir()]
			if len(members) > MAX_MEMBERS:
				raise UnsafeArchiveError(
					f"{zip_name} holds {len(members)} files, over the "
					f"{MAX_MEMBERS} allowed in one archive."
				)

			to_extract, skipped_names, declared_total = [], [], 0
			for member in members:
				if member.flag_bits & _ENCRYPTED_FLAG:
					raise UnsafeArchiveError(
						f"{zip_name} is password-protected, so its contents "
						f"cannot be read. Upload it unencrypted."
					)
				_reject_non_regular(member)

				# Checked before the suffix filter: a member reaching out of the
				# destination is a property of the archive, not of one file in it,
				# and it is not made safe by our having ignored that file.
				relative_path = _safe_relative_path(member.filename)
				if relative_path is None:
					continue

				if not _is_importable(relative_path.name):
					skipped_names.append(member.filename)
					continue

				if member.file_size > MAX_MEMBER_BYTES:
					raise UnsafeArchiveError(
						f"Archive member {member.filename!r} expands to "
						f"{member.file_size / GIB:.1f} GB, over the "
						f"{MAX_MEMBER_BYTES / GIB:.0f} GB allowed for one file."
					)
				if (
					member.compress_size >= RATIO_FLOOR_BYTES
					and member.file_size > member.compress_size * MAX_COMPRESSION_RATIO
				):
					raise UnsafeArchiveError(
						f"Archive member {member.filename!r} expands "
						f"{member.file_size // max(member.compress_size, 1)}x, over the "
						f"{MAX_COMPRESSION_RATIO}x this accepts; refused as a possible zip bomb."
					)

				declared_total += member.file_size
				if declared_total > MAX_TOTAL_BYTES:
					raise UnsafeArchiveError(
						f"{zip_name} expands to more than "
						f"{MAX_TOTAL_BYTES / GIB:.0f} GB, which this refuses to unpack."
					)
				to_extract.append((member, relative_path))

			dest_dir.mkdir(parents=True, exist_ok=True)
			resolved_dest = dest_dir.resolve()
			free_bytes = shutil.disk_usage(resolved_dest).free
			if declared_total + FREE_DISK_HEADROOM_BYTES > free_bytes:
				raise UnsafeArchiveError(
					f"{zip_name} needs {declared_total / GIB:.1f} GB unpacked but only "
					f"{free_bytes / GIB:.1f} GB is free. Upload it as a folder instead, "
					f"which is sent in batches."
				)

			written_total = 0
			for member, relative_path in to_extract:
				destination_path = dest_dir / relative_path
				# Belt and braces over _safe_relative_path: whatever the name did,
				# what actually gets written has to land inside the destination.
				if not destination_path.resolve().is_relative_to(resolved_dest):
					raise UnsafeArchiveError(
						f"Archive member tries to escape the upload folder: "
						f"{member.filename!r}"
					)
				destination_path.parent.mkdir(parents=True, exist_ok=True)
				written_total += _copy_member(archive, member, destination_path)
				if written_total > MAX_TOTAL_BYTES:
					raise UnsafeArchiveError(
						f"{zip_name} unpacked to more than "
						f"{MAX_TOTAL_BYTES / GIB:.0f} GB, which this refuses to unpack."
					)
	except zipfile.BadZipFile as exception:
		# Also where a member that read back longer or differently than its header
		# promised ends up: the CRC check fails as the last chunk arrives.
		shutil.rmtree(dest_dir, ignore_errors=True)
		raise UnsafeArchiveError(
			f"{zip_name} is damaged and could not be read ({exception})."
		) from exception
	except Exception:
		# Including UnsafeArchiveError: a refused archive leaves nothing behind.
		shutil.rmtree(dest_dir, ignore_errors=True)
		raise

	if skipped_names:
		examples = ", ".join(sorted(skipped_names)[:3])
		warnings.append(
			f"{zip_name}: skipped {len(skipped_names)} file(s) that are not reads, "
			f"a stats workbook or a checksum list (e.g. {examples})."
		)
	if not to_extract:
		warnings.append(f"{zip_name} contained no files this could import.")

	return len(to_extract)
