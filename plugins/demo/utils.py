import argparse
import json
import os
import random
import shutil
import tarfile
from abc import ABC, abstractmethod
from collections.abc import Callable
from gzip import GzipFile
from typing import Any

import xmltodict

"""
To rebuild the dataset follow the steps described below:

1)
Download the data:
    - https://metabrainz.org/datasets/postgres-dumps#musicbrainz-json
    - https://data.discogs.com/
    - https://github.com/marko-knoebl/chinook-database-json

2)
Dump all raw dataset files into a folder named polypheny_demo like this:
polypheny_demo/
├── relational/
│   ├── *.json
├── graph/
│   ├── *.tar.xz
├── document/
│   ├── *.xml.gz

Files do not need to be uncompressed.

3)
Then run the script with
```
python utils.py --src <path>/polypheny_demo --dst <output_path>
```
"""

class Dataset(ABC):
    src_dir: str
    dst_dir: str
    artists: set[str]

    def __init__(self, src_dir: str, dst_dir: str):
        self.src_dir = src_dir
        self.dst_dir = dst_dir

        if not os.path.exists(self.dst_dir):
            os.mkdir(self.dst_dir)

        self.artists = set()

    def _iterate_file(self, file_name: str, key: str, f: Callable[[dict], Any]):
        path = f"{self.src_dir}/{file_name}"
        print(f"Loading file {path}")

        if path.endswith(".json"):
            with open(path, "r") as file:
                j = json.load(file)
                for line in j:
                    f(line)

        elif path.endswith(".tar.xz"):
            with tarfile.open(path, "r:xz") as tar:
                for member in tar.getmembers():
                    print(member)
                    if member.name.startswith("mbdump"):
                        t = tar.extractfile(member)
                        if t is None:
                            return
                        for line in t:
                            j = json.loads(line)
                            f(j)

        elif path.endswith(".xml.gz"):
            def handle(_, j):
                f(j)
                return True
            xmltodict.parse(GzipFile(path), item_depth=2, item_callback=handle)

    def _create_file(self, file: str):
        open(file, "x").close()

    def mv_files(self, files: tuple[str, str] | list[tuple[str, str]]):
        if type(files) is tuple:
            files = [files]
        for file in files:
            shutil.copy(file[0], file[1])

    @abstractmethod
    def create_metafile(self):
        ...

    @abstractmethod
    def expand_artists(self, artists: set[str]) -> set[str]:
        ...

    @abstractmethod
    def create_reduced_files(self):
        ...


class Chinook(Dataset):
    def __init__(self, src_dir: str, dst_dir: str):
        src_dir = f"{src_dir}/relational"
        dst_dir = f"{dst_dir}/relational"

        super().__init__(src_dir, dst_dir)

    def load_artists(self) -> set[str]:
        def handle(j: dict):
            self.artists.add(j["Name"])
        super()._iterate_file("Artist.json", "Name", handle)
        return self.artists

    def create_metafile(self):
        return super()._create_file(f"{self.dst_dir}/__CHINOOK__")

    def expand_artists(self, artists: set[str]) -> set[str]:
        return self.artists

    def create_reduced_files(self):
        files = [(f"{self.src_dir}/{file_name}", f"{self.dst_dir}/{file_name}") for file_name in os.listdir(self.src_dir)]
        self.mv_files(files)

class Musicbrainz(Dataset):
    def __init__(self, src_dir: str, dst_dir: str):
        src_dir = f"{src_dir}/graph"
        dst_dir = f"{dst_dir}/graph"
        self.top_artists = set()
        super().__init__(src_dir, dst_dir)

    def load_artists(self) -> set[str]:
        def handle(j: dict):
            if "name" in j:
                name = j["name"]
                self.artists.add(name)
                if j["rating"]["votes-count"] > 20:
                    self.top_artists.add(name)

        for file_name in os.listdir(self.src_dir):
            if "artist" in file_name:
                super()._iterate_file(file_name, "name", handle)

        return self.artists

    def create_metafile(self):
        return super()._create_file(f"{self.dst_dir}/__MUSICBRAINZ__")

    def expand_artists(self, artists: set[str]) -> set[str]:
        self.artists = artists.union(self.top_artists)
        return self.artists

    def create_reduced_files(self):
        for file_name in os.listdir(self.src_dir):
            if "artist" in file_name:
                artists = []
                def reduce_top_artists(j: dict, artists=artists):
                    if j["name"] in self.artists:
                        artists.append(f"{json.dumps(j)}\n")
                super()._iterate_file(file_name, "name", reduce_top_artists)
                with open(f"{self.dst_dir}/artist.jsonl", "w") as file:
                    file.writelines(artists)

class Discogs(Dataset):
    def __init__(self, src_dir: str, dst_dir: str):
        src_dir = f"{src_dir}/document"
        dst_dir = f"{dst_dir}/document"
        super().__init__(src_dir, dst_dir)

    def load_artists(self) -> set[str]:
        def handle(j: dict):
            if "name" in j:
                self.artists.add(j["name"])

        for file_name in os.listdir(self.src_dir):
            if "artist" in file_name:
                super()._iterate_file(file_name, "name", handle)

        return self.artists

    def create_metafile(self):
        return super()._create_file(f"{self.dst_dir}/__DISCOGS__")

    def expand_artists(self, artists: set[str]) -> set[str]:
        random_artists = set(random.sample(list(self.artists), 100))
        self.artists = artists.union(random_artists)
        return self.artists

    def create_reduced_files(self):
        for file_name in os.listdir(self.src_dir):
            if "artist" in file_name:
                artists = []
                def handle(j: dict, artists=artists):
                    if j["name"] in self.artists:
                        artists.append(f"{json.dumps(j)}\n")
                with open(f"{self.dst_dir}/artist.jsonl", "w") as file:
                    file.writelines(artists)

def parse_musicbrainz(src: str, dst: str):
        top_artists = []
        relations = []
        with open(src, "r") as json_file:
            for line in json_file:
                data = json.loads(line)
                if data["rating"]["votes-count"] > 20:
                    data.pop("aliases")
                    data.pop("disambiguation")
                    data.pop("sort-name")
                    for relation in data.pop("relations"):
                        relations.append(f"{json.dumps(relation)}\n")
                    top_artists.append(f"{json.dumps(data)}\n")

        with open(dst, "w") as json_file:
            json_file.writelines(top_artists)

        with open("relations.json", "w") as json_file:
            json_file.writelines(relations)

def parse_datasets(src_dir: str, dst_dir: str):
    RELATIONAL = Chinook(src_dir, dst_dir)
    GRAPH = Musicbrainz(src_dir, dst_dir)
    DOCUMENT = Discogs(src_dir, dst_dir)

    relational_artists = RELATIONAL.load_artists()
    graph_artists = GRAPH.load_artists()
    document_artists = DOCUMENT.load_artists()

    common_artists: set[str] = relational_artists.intersection(graph_artists, document_artists)

    relational_artists = RELATIONAL.expand_artists(common_artists)
    graph_artists = GRAPH.expand_artists(common_artists)
    document_artists = DOCUMENT.expand_artists(common_artists)

    print(f"Total artists (Relational: {len(relational_artists)}, Graph: {graph_artists}, Document: {document_artists})")

    RELATIONAL.create_reduced_files()
    GRAPH.create_reduced_files()
    DOCUMENT.create_reduced_files()

    RELATIONAL.create_metafile()
    GRAPH.create_metafile()
    DOCUMENT.create_metafile()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", type=str, required=False)
    parser.add_argument("--dst-dir", type=str, required=False)

    args = parser.parse_args()

    parse_datasets(args.src_dir, args.dst_dir)
