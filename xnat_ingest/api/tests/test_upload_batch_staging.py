from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

from xnat_ingest.api.upload_api import _staged_upload_batch


def test_concurrent_upload_batches_use_distinct_directories(tmp_path: Path) -> None:
    source_dir = tmp_path / "scan" / "secondary"
    source_dir.mkdir(parents=True)
    source_files = [source_dir / "one.ima", source_dir / "two.ima"]
    for source_file in source_files:
        source_file.write_bytes(source_file.name.encode())

    barrier = Barrier(len(source_files))

    def stage(source_file: Path) -> tuple[Path, set[str]]:
        with _staged_upload_batch(source_dir, [source_file]) as upload_dir:
            barrier.wait()
            return upload_dir, {path.name for path in upload_dir.iterdir()}

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(stage, source_files))

    upload_dirs = [upload_dir for upload_dir, _ in results]
    assert upload_dirs[0] != upload_dirs[1]
    assert [names for _, names in results] == [{"one.ima"}, {"two.ima"}]
    assert all(not upload_dir.exists() for upload_dir in upload_dirs)
