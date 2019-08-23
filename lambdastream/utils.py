import random
import string

from lambdastream.constants import DONE_MARKER


def random_string(length=5):
    """Generate a random string of fixed length """
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(length))


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
