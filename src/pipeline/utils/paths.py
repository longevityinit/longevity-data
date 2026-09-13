"""Output locations shared by the pipeline stages."""
from pathlib import Path


def get_output_root(repository_root: Path) -> Path:
    """Keep local artifacts separate from the normal publication inputs."""
    return repository_root / "output"
