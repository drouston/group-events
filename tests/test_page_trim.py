"""page_trim.remove_repeats. Run: python tests/test_page_trim.py"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import page_trim  # noqa: E402

DESC = 'A long description paragraph that repeats on every date of a recurring show, well over the limit. ' * 2


def test_repeated_description_kept_once_short_lines_kept():
    text = '\n'.join(['Fri Dec 4', 'Show', DESC, 'Fri Dec 4', 'Show', DESC])
    assert page_trim.remove_repeats(text).split('\n') == ['Fri Dec 4', 'Show', DESC, 'Fri Dec 4', 'Show']


def test_link_lines_collapse_only_when_close():
    link = '[https://t/1]'
    near = '\n'.join([link, 'Buy [https://t/1]', link])
    assert page_trim.remove_repeats(near).split('\n') == [link, 'Buy [https://t/1]']
    far = '\n'.join([link] + [f'line {i}' for i in range(page_trim.LINK_WINDOW)] + [link])
    assert page_trim.remove_repeats(far).split('\n')[-1] == link


def test_back_to_back_copies_collapse():
    assert page_trim.remove_repeats('Sat Dec 5\nSat Dec 5\nShow') == 'Sat Dec 5\nShow'


if __name__ == '__main__':
    for t in (test_repeated_description_kept_once_short_lines_kept, test_link_lines_collapse_only_when_close,
              test_back_to_back_copies_collapse):
        t()
        print('PASS', t.__name__)
