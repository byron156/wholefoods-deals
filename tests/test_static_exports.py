import gzip
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from build_static_site import write_json_export


class StaticExportsTest(unittest.TestCase):
    def test_large_archive_preserves_every_record(self):
        records = [{'asin': str(i), 'evidence': 'source observation ' * 100} for i in range(100)]
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'source.json'
            source.write_text(json.dumps(records))
            target = Path(directory) / 'catalog.json'
            with patch('build_static_site.MAX_ASSET_BYTES', 8000):
                written = write_json_export(source, target)
            self.assertEqual(written.name, 'catalog.json.gz')
            self.assertLess(written.stat().st_size, 8000)
            self.assertEqual(json.loads(gzip.decompress(written.read_bytes())), records)
            self.assertFalse(target.exists())

    def test_small_export_keeps_json_url(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'source.json'
            source.write_text('[{"asin":"123"}]')
            target = Path(directory) / 'catalog.json'
            self.assertEqual(write_json_export(source, target), target)
            self.assertEqual(json.loads(target.read_text()), [{'asin': '123'}])
