"""Uploading a run folder as one .zip, and refusing the archives that are not one.

An uploaded archive is untrusted input that the server opens on its own disk, so
these cover both halves of that: a real delivery in a zip has to import exactly as
the same files loose in a folder do -- same pairing, same checksum verification,
same manifest -- and an archive that tries to escape the staging directory, hide a
symlink, lie about its size or smuggle in files the importer never asked for has to
be refused with a reason, leaving nothing behind.

The hostile archives here are built by hand rather than with ZipFile.write(),
because ZipFile.write() will not produce them: the member names, modes and size
headers being tested are exactly the fields it fills in correctly.
"""

import io
import stat
import struct
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

from tests._isolation import TMP_ROOT  # noqa: F401  (must import first)
from tests.test_batching import Base  # noqa: E402
from tests.test_cloud_import import fastq_bytes, md5, stats_workbook  # noqa: E402
from workflow.helpers import archive_import, jobs  # noqa: E402
from workflow.helpers.archive_import import UnsafeArchiveError, extract_zip  # noqa: E402


def zip_bytes(members, root=""):
	"""A zip of {name: bytes}, optionally under a top-level folder."""
	buf = io.BytesIO()
	with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as archive:
		for member_name, payload in members.items():
			archive.writestr(f"{root}{member_name}", payload)
	return buf.getvalue()


def pair_members(name, records=1):
	"""One sample's two reads, named the way the sequencing company names them."""
	return {
		f"{name}_S1_R1_001.fastq.gz": fastq_bytes(records, "r1"),
		f"{name}_S1_R2_001.fastq.gz": fastq_bytes(records, "r2"),
	}


def zip_with_member(member_name, payload=b"data", external_attr=None):
	"""A zip holding one member, with its Unix mode forced to a chosen value --
	which is how a hostile archive says "this one is a symlink"."""
	buf = io.BytesIO()
	with zipfile.ZipFile(buf, "w") as archive:
		info = zipfile.ZipInfo(member_name)
		if external_attr is not None:
			info.external_attr = external_attr
		archive.writestr(info, payload)
	return buf.getvalue()


def with_encrypted_flag(archive_bytes):
	"""Set bit 0 of the general-purpose flags -- "this member is encrypted".

	Written into the bytes rather than the ZipInfo because ZipFile will not write
	an encrypted member and clears the flag if you ask it to; the archives this
	has to recognize were made by something else.
	"""
	tampered = bytearray(archive_bytes)
	local_header = tampered.index(b"PK\x03\x04")
	central_directory = tampered.rindex(b"PK\x01\x02")
	tampered[local_header + 6] |= 0x1
	tampered[central_directory + 8] |= 0x1
	return bytes(tampered)


class TestArchiveSafety(unittest.TestCase):
	"""extract_zip on its own: what it refuses, and what it leaves behind."""

	def setUp(self):
		self.dest = TMP_ROOT / "unpack" / self.id().rsplit(".", 1)[-1]
		self.warnings = []

	def extract(self, archive_bytes):
		return extract_zip(io.BytesIO(archive_bytes), self.dest, self.warnings, display_name="d.zip")

	def test_a_normal_run_folder_unpacks(self):
		written = self.extract(zip_bytes(pair_members("SW1"), root="Run1/"))
		self.assertEqual(written, 2)
		self.assertEqual(
			sorted(path.name for path in self.dest.rglob("*.gz")),
			["SW1_S1_R1_001.fastq.gz", "SW1_S1_R2_001.fastq.gz"],
		)

	def test_traversal_member_is_refused_and_writes_nothing(self):
		outside = Path(self.dest).parent / "escaped.fastq.gz"
		with self.assertRaises(UnsafeArchiveError) as raised:
			self.extract(zip_bytes({"../escaped.fastq.gz": fastq_bytes()}))
		self.assertIn("escape", str(raised.exception))
		self.assertFalse(outside.exists(), "a traversal member must not be written")
		self.assertFalse(self.dest.exists(), "a refused archive must leave nothing behind")

	def test_absolute_member_is_refused(self):
		with self.assertRaises(UnsafeArchiveError):
			self.extract(zip_bytes({"/etc/cron.d/payload.txt": b"x"}))

	def test_windows_drive_letter_is_refused(self):
		with self.assertRaises(UnsafeArchiveError):
			self.extract(zip_bytes({"C:\\Windows\\reads.fastq": b"x"}))

	def test_symlink_member_is_refused(self):
		symlink_attr = (stat.S_IFLNK | 0o777) << 16
		with self.assertRaises(UnsafeArchiveError) as raised:
			self.extract(zip_with_member("reads.fastq", b"/etc", external_attr=symlink_attr))
		self.assertIn("symlink", str(raised.exception))

	def test_a_plain_unix_file_is_not_mistaken_for_a_special_one(self):
		regular_attr = (stat.S_IFREG | 0o644) << 16
		self.extract(zip_with_member("SW1_S1_R1_001.fastq", fastq_bytes(), regular_attr))
		self.assertTrue((self.dest / "SW1_S1_R1_001.fastq").exists())

	def test_encrypted_archive_is_refused_rather_than_half_read(self):
		with self.assertRaises(UnsafeArchiveError) as raised:
			self.extract(with_encrypted_flag(zip_bytes({"reads.fastq": b"x" * 64})))
		self.assertIn("password", str(raised.exception))

	def test_a_bomb_is_refused_on_its_compression_ratio(self):
		# 8 MB of zeros deflates to a few KB: ~1000x, where real reads are ~1x.
		with self.assertRaises(UnsafeArchiveError) as raised:
			self.extract(zip_bytes({"bomb.fastq": b"\0" * (8 * 1024 * 1024)}))
		self.assertIn("zip bomb", str(raised.exception))

	def test_an_oversized_member_is_refused_before_anything_is_written(self):
		with mock.patch.object(archive_import, "MAX_MEMBER_BYTES", 16):
			with self.assertRaises(UnsafeArchiveError) as raised:
				self.extract(zip_bytes({"big.fastq": b"ACGT" * 64}))
		self.assertIn("over the", str(raised.exception))
		self.assertFalse(self.dest.exists())

	def test_a_member_that_lies_about_its_size_is_refused(self):
		"""Every size check above is made against the header, so the header is the
		thing a hostile archive would lie about. Whichever guard fires first --
		the streaming ceiling here, or the CRC the truncated read fails -- the
		archive is refused and nothing of it is kept."""
		honest = zip_bytes({"reads.fastq": b"ACGT" * 4096})
		# Rewrite the central directory's uncompressed-size field to 4 bytes. The
		# member still expands to 16 KB; nothing in the headers says so any more.
		tampered = bytearray(honest)
		central_directory = tampered.rindex(b"PK\x01\x02")
		struct.pack_into("<I", tampered, central_directory + 24, 4)
		with self.assertRaises(UnsafeArchiveError):
			self.extract(bytes(tampered))
		self.assertFalse(self.dest.exists())

	def test_files_the_importer_would_never_read_are_left_in_the_archive(self):
		members = {
			**pair_members("SW1"),
			"nested.zip": b"PK\x03\x04nope",
			"install.sh": b"#!/bin/sh\nrm -rf /\n",
			"report.pdf": b"%PDF-1.4",
		}
		written = self.extract(zip_bytes(members))
		self.assertEqual(written, 2)
		self.assertEqual(
			sorted(path.name for path in self.dest.rglob("*") if path.is_file()),
			["SW1_S1_R1_001.fastq.gz", "SW1_S1_R2_001.fastq.gz"],
		)
		self.assertIn("skipped 3 file(s)", " ".join(self.warnings))

	def test_too_many_members_is_refused(self):
		crowded = dict(pair_members("SW1"), **{"extra.txt": b"x"})
		with mock.patch.object(archive_import, "MAX_MEMBERS", 2):
			with self.assertRaises(UnsafeArchiveError) as raised:
				self.extract(zip_bytes(crowded))
		self.assertIn("over the", str(raised.exception))

	def test_an_archive_too_big_for_the_disk_is_refused_before_it_fills_it(self):
		with mock.patch.object(archive_import, "FREE_DISK_HEADROOM_BYTES", 1 << 62):
			with self.assertRaises(UnsafeArchiveError) as raised:
				self.extract(zip_bytes(pair_members("SW1")))
		self.assertIn("free", str(raised.exception))
		self.assertFalse(self.dest.exists())

	def test_a_damaged_archive_is_refused(self):
		with self.assertRaises(UnsafeArchiveError) as raised:
			self.extract(b"PK\x03\x04 this is not really a zip")
		self.assertIn("not a readable .zip", str(raised.exception))


class TestZipUpload(Base):
	"""The same archives through the real /import endpoint."""

	def import_files(self, files, job_id=None):
		data = {"files": files}
		if job_id:
			data["job_id"] = job_id
		return self.client.post("/import", data=data, content_type="multipart/form-data")

	def test_a_zipped_run_folder_registers_its_samples(self):
		archive = zip_bytes(pair_members("ZIPPED"), root="Run1/")
		response = self.import_files([(io.BytesIO(archive), "Run1.zip")])
		self.assertEqual(response.status_code, 200, response.get_json())
		self.assertEqual(self.isolates(response.get_json()["job_id"]), ["ZIPPED_S1"])

	def test_a_zip_is_verified_against_the_workbook_it_carries(self):
		first_read, second_read = fastq_bytes(1, "r1"), fastq_bytes(1, "r2")
		archive = zip_bytes(
			{
				"VERIFIED_S1_R1_001.fastq.gz": first_read,
				"VERIFIED_S1_R2_001.fastq.gz": second_read,
				"DNA Sequencing Stats.xlsx": stats_workbook(
					[["VERIFIED", md5(first_read), md5(second_read)]]
				),
			}
		)
		payload = self.import_files([(io.BytesIO(archive), "Run.zip")]).get_json()
		self.assertEqual(payload["verified"], ["VERIFIED_S1"])

	def test_a_zip_whose_checksums_disagree_is_not_imported(self):
		archive = zip_bytes(
			{
				**pair_members("BADSUM"),
				"DNA Sequencing Stats.xlsx": stats_workbook([["BADSUM", md5(b"other"), md5(b"x")]]),
			}
		)
		payload = self.import_files([(io.BytesIO(archive), "Run.zip")]).get_json()
		self.assertEqual(payload["added"], [])
		self.assertEqual(payload["failed"], ["BADSUM_S1"])

	def test_a_zip_and_loose_reads_fill_one_job_together(self):
		job_id = self.import_files(
			[(io.BytesIO(zip_bytes(pair_members("FROMZIP"))), "Run.zip")]
		).get_json()["job_id"]
		self.import_folder(["FROMFOLDER"], job_id=job_id)
		self.assertEqual(self.isolates(job_id), ["FROMFOLDER", "FROMZIP_S1"])

	def test_a_hostile_zip_is_refused_with_a_reason_and_no_job_samples(self):
		archive = zip_bytes({"../../escaped.fastq.gz": fastq_bytes()})
		response = self.import_files([(io.BytesIO(archive), "Run.zip")])
		self.assertEqual(response.status_code, 400)
		self.assertIn("escape", response.get_json()["error"])
		self.assertFalse((jobs.PROJECT_ROOT / "escaped.fastq.gz").exists())
		self.assertFalse((Path(tempfile.gettempdir()) / "escaped.fastq.gz").exists())

	def test_a_hostile_zip_does_not_take_the_reads_beside_it_down_with_it(self):
		"""The archive is refused, and the request with it -- so the loose pair in
		the same request is not registered either. What must not happen is a 500,
		or a job left holding half of a rejected upload."""
		response = self.import_files(
			[
				(io.BytesIO(fastq_bytes()), "Run/GOOD_S1_R1_001.fastq.gz"),
				(io.BytesIO(fastq_bytes()), "Run/GOOD_S1_R2_001.fastq.gz"),
				(io.BytesIO(zip_bytes({"../escape.fastq.gz": fastq_bytes()})), "Run/bad.zip"),
			]
		)
		self.assertEqual(response.status_code, 400)

	def test_two_zips_in_one_upload_do_not_share_a_directory(self):
		archives = [
			(io.BytesIO(zip_bytes(pair_members("TWINA"), root="Run/")), "Run.zip"),
			(io.BytesIO(zip_bytes(pair_members("TWINB"), root="Run/")), "Run.zip"),
		]
		payload = self.import_files(archives).get_json()
		self.assertEqual(sorted(payload["added"]), ["TWINA_S1", "TWINB_S1"])

	def test_what_the_archive_skipped_is_reported_to_the_user(self):
		archive = zip_bytes({**pair_members("SKIPS"), "notes.pdf": b"%PDF-1.4"})
		payload = self.import_files([(io.BytesIO(archive), "Run.zip")]).get_json()
		self.assertTrue(any("notes.pdf" in warning for warning in payload["warnings"]))
		self.assertEqual(payload["skipped"], len(payload["warnings"]))


if __name__ == "__main__":
	unittest.main()
