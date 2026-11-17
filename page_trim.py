"""Page trimming before LLM extraction. Per venue via trim_options in universal_scraper:
`trim_duplicate_text` (default on) and `trim_hidden_text` (default off).

  1. Hidden elements: the browser marks every element with display:none at a fixed desktop
     window size, and those are removed before the page becomes text.
  2. Repeated text: long lines (descriptions) keep only their first copy page-wide; bare link
     lines are dropped only when the same line appeared a few lines earlier; back-to-back
     identical lines collapse. Short lines (dates, times, titles) are never removed otherwise.
"""
import re

WINDOW_SIZE = '1920,1080'
HIDDEN_ATTR = 'data-trim-hidden'

# display:none only; a visibility:hidden parent can have visible children, display:none can't.
MARK_HIDDEN_JS = f"""
var n = 0;
document.querySelectorAll('body *').forEach(function (el) {{
  if (getComputedStyle(el).display === 'none') {{ el.setAttribute('{HIDDEN_ATTR}', '1'); n++; }}
}});
return [n, window.innerWidth];
"""

LONG_LINE = 120
LINK_WINDOW = 4
_LINK_ONLY = re.compile(r'^\[[^\]\s]+\]$')


def drop_hidden(soup):
    """Remove elements the browser marked as not displayed. Returns how many were removed."""
    marked = soup.select(f'[{HIDDEN_ATTR}]')
    for el in marked:
        if not el.decomposed:
            el.decompose()
    return len(marked)


def remove_repeats(text):
    """Drop repeated description paragraphs, nearby repeated link lines and back-to-back copies."""
    seen_long = set()
    out = []
    for line in text.split('\n'):
        if out and line == out[-1]:
            continue
        if len(line) >= LONG_LINE:
            if line in seen_long:
                continue
            seen_long.add(line)
        elif _LINK_ONLY.match(line) and line in out[-LINK_WINDOW:]:
            continue
        out.append(line)
    return '\n'.join(out)
