from operator.constants import DONE_MARKER


class RangeSource(object):
    def __init__(self, begin, end, step=1):
        self.current = begin
        self.end = end
        self.step = step

    def __call__(self):
        if self.current >= self.end:
            return DONE_MARKER
        value = self.current
        self.current += self.step
        return value
