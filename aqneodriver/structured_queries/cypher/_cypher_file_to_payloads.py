from typing import List


def parse_cypher_file(filename: str) -> List[str]:
    with open(filename) as f:
        lines = [line.strip() for line in f.readlines()]
        queries = [[]]
        for line in lines:
            if line == "":
                queries.append([])
            else:
                queries[-1].append(line)
        queries = ["\n".join(lines) for lines in queries if lines]
    return queries
