import random
import string


def random_string(length=5):
    """Generate a random string of fixed length """
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(length))
