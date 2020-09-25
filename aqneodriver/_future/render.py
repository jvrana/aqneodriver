from dataclasses import dataclass, field
from typing import List, Dict
import re
from rich.syntax import Syntax
from rich import print
from rich.panel import Panel


@dataclass
class Block(object):
    lines: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    indent: int = None
    directive_pattern = re.compile('^\s*\.\.\s(?P<directive>[a-zA-Z-_]+)::(?:\s(?P<mod>[a-zA-Z-_]+))?')

    @staticmethod
    def lindent(s: str):
        return len(s) - len(s.lstrip())

    def __str__(self):
        if self.indent is not None:
            indents = [self.lindent(l) for l in self.lines]
            indents = [i - min(indents) + self.indent for i in indents]
            lines = [' '*i + l.lstrip() for i, l in zip(indents, self.lines)]
        else:
            lines = self.lines
        return '\n'.join(lines)

    @classmethod
    def collect(cls, s: str):
        block_indent = 0
        blocks = [cls()]
        lineiter = iter(s.splitlines())
        try:
            while True:
                line = next(lineiter)
                curr_indent = cls.lindent(line)
                if curr_indent < block_indent:
                    blocks.append(cls())
                match = cls.directive_pattern.match(line)
                if match:
                    tags = match.groupdict()
                    blocks.append(Block([], tags, indent=curr_indent))
                    next(lineiter)
                    line = next(lineiter)
                    block_indent = cls.lindent(line)
                blocks[-1].lines.append(line)
        except StopIteration:
            pass
        return blocks

    def render(self):
        if self.tags.get('directive', '') == 'code-block':
            lang = self.tags.get('mod', 'python')
            print(lang)
            syntax = Syntax(str(self), lang, line_numbers=True)
            return Panel(syntax, title='{} code'.format(lang))
        elif self.tags.get('directive', '') == 'note':
            title = self.tags.get('mod', self.tags['directive'])
            return Panel(str(self), style='black on white', title=title)
        elif self.tags.get('directive', '') == 'warning':
            title = self.tags.get('mod', self.tags['directive'])
            return Panel(str(self), style='white on red', title=title)
        else:
            return str(self)


def render_rst(rst):
    for block in Block.collect(rst):
        print(block.render())