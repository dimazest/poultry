import fileinput
import contextlib

from poultry.utils import get_file_names
from poultry import consumers


def consume_stdin_or_dir(target, input_dir=None):
    """Read lines from the standard input or files in a directory.

    Behaves as a generator, should receive a .send() call to send a
    line to the target.

    """
    file_names = get_file_names(input_dir) if input_dir else []

    with contextlib.closing(fileinput.FileInput(file_names, openhook=fileinput.hook_compressed)) as lines:
        with consumers.closing(target):

            for line in lines:
                result = target.send(line)

                if result is not consumers.SendNext:
                    yield result


def from_stdin_or_dir(target, input_dir=None):
    """Send lines from the standard input or from the input directory to the target.

    :param target: a generator to which the read lines are sent.
    :param input_dir: the path to a directory with tweet files.

    """
    consumer = consume_stdin_or_dir(target, input_dir)

    try:
        while True:
            next(consumer)
    except StopIteration:
        pass


def from_simple_queue(target, queue):
    with consumers.closing(target):
        while True:
            item = queue.get()

            if item is StopIteration:
                break

            target.send(item)

