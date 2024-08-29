# -*- coding: utf-8 -*-
"""
Opens the `constants.py` file and updates the
`DOCKER_TAG` variable with the provided argument.
"""

from pathlib import Path
from sys import argv, exit
from typing import List

FILE_PATH = Path("./pipelines/constants.py")
REPLACE_TAG = "AUTO_REPLACE_DOCKER_TAG"
REPLACE_IMAGE = "AUTO_REPLACE_DOCKER_IMAGE"
REPLACE_FEDORA_IMAGE = "AUTO_REPLACE_FEDORA_IMAGE"
REPLACE_FEDORA_TAG = "AUTO_REPLACE_FEDORA_TAG"


def get_name_version_from_args() -> List[str]:
    """
    Returns the version from the command line arguments.
    """
    if len(argv) != 4:
        print("Usage: replace_docker_tag.py <debian/fedora> <image_name> <version>")
        exit(1)
    return argv[1], argv[2], argv[3]


def replace_in_text(orig_text: str, find_text: str, replace_text: str) -> str:
    """
    Replaces the `find_text` with `replace_text` in the `orig_text`.
    """
    return orig_text.replace(find_text, replace_text)


def update_file(file_path: Path, image_name: str, version: str, mode: str = None) -> None:
    """
    Updates the `DOCKER_TAG` variable in the `constants.py` file.
    """
    with file_path.open("r") as file:
        text = file.read()
    if mode.lower() == "fedora":
        replace_tag = REPLACE_FEDORA_TAG
        replace_image = REPLACE_FEDORA_IMAGE
    else:
        replace_tag = REPLACE_TAG
        replace_image = REPLACE_IMAGE
    print(f"Will replace {replace_image}:{replace_tag} -> {image_name}:{version}")
    text = replace_in_text(text, replace_tag, version)
    text = replace_in_text(text, replace_image, image_name)
    with file_path.open("w") as file:
        file.write(text)


if __name__ == "__main__":
    mode, image_name, version = get_name_version_from_args()
    update_file(FILE_PATH, image_name, version, mode=mode)
