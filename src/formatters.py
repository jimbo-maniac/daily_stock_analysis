# -*- coding: utf-8 -*-
"""
===================================
formattingtoolmodule
===================================

provideeachtypecontentformattingutility function，forwillgenericformatconvertingasplatformspecificformat。
"""

import re
from typing import List

import markdown2

TRUNCATION_SUFFIX = "\n\n...(thissegmentcontentlongalreadytruncate)"
PAGE_MARKER_PREFIX = f"\n\n📄"
PAGE_MARKER_SAFE_BYTES = 16 # "\n\n📄 9999/9999"
PAGE_MARKER_SAFE_LEN = 13   # "\n\n📄 9999/9999"
MIN_MAX_WORDS = 10
MIN_MAX_BYTES = 40

# Unicode code point ranges for special characters.
_SPECIAL_CHAR_RANGE = (0x10000, 0xFFFFF)
_SPECIAL_CHAR_REGEX = re.compile(r'[\U00010000-\U000FFFFF]')


def _page_marker(i: int, total: int) -> str:
    return f"{PAGE_MARKER_PREFIX} {i+1}/{total}"


def _is_special_char(c: str) -> bool:
    """determinecharacterwhether isspecialcharacter
    
    Args:
        c: character
        
    Returns:
        True ifcharacterasspecialcharacter，False otherwise
    """
    if len(c) != 1:
        return False
    cp = ord(c)
    return _SPECIAL_CHAR_RANGE[0] <= cp <= _SPECIAL_CHAR_RANGE[1]


def _count_special_chars(s: str) -> int:
    """
    calculatingstringinspecialcharacterquantity
    
    Args:
        s: string
    """
    # reg find all (0x10000, 0xFFFFF)
    match = _SPECIAL_CHAR_REGEX.findall(s)
    return len(match)


def _effective_len(s: str, special_char_len: int = 2) -> int:
    """
    calculatingstringvalidlength
    
    Args:
        s: string
        special_char_len: length per special character，defaultas 2
        
    Returns:
        s validlength
    """
    n = len(s)
    n += _count_special_chars(s) * (special_char_len - 1)
    return n


def _slice_at_effective_len(s: str, effective_len: int, special_char_len: int = 2) -> tuple[str, str]:
    """
    byvalidlengthsplittingstring
    
    Args:
        s: string
        effective_len: validlength
        special_char_len: length per special character，defaultas 2
        
    Returns:
        splittingafterbefore、afterpartialstring
    """
    if _effective_len(s, special_char_len) <= effective_len:
        return s, ""
    
    s_ = s[:effective_len]
    n_special_chars = _count_special_chars(s_)
    residual_lens = n_special_chars * (special_char_len - 1) + len(s_) - effective_len
    while residual_lens > 0:
        residual_lens -= special_char_len if _is_special_char(s_[-1]) else 1
        s_ = s_[:-1]
    return s_, s[len(s_):]


def markdown_to_html_document(markdown_text: str) -> str:
    """
    Convert Markdown to a complete HTML document (for email, md2img, etc.).

    Uses markdown2 with table and code block support, wraps with inline CSS
    for compact, readable layout. Reused by notification email and md2img.

    Args:
        markdown_text: Raw Markdown content.

    Returns:
        Full HTML document string with DOCTYPE, head, and body.
    """
    html_content = markdown2.markdown(
        markdown_text,
        extras=["tables", "fenced-code-blocks", "break-on-newline", "cuddled-lists"],
    )

    css_style = """
            body {
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
                line-height: 1.5;
                color: #24292e;
                font-size: 14px;
                padding: 15px;
                max-width: 900px;
                margin: 0 auto;
            }
            h1 {
                font-size: 20px;
                border-bottom: 1px solid #eaecef;
                padding-bottom: 0.3em;
                margin-top: 1.2em;
                margin-bottom: 0.8em;
                color: #0366d6;
            }
            h2 {
                font-size: 18px;
                border-bottom: 1px solid #eaecef;
                padding-bottom: 0.3em;
                margin-top: 1.0em;
                margin-bottom: 0.6em;
            }
            h3 {
                font-size: 16px;
                margin-top: 0.8em;
                margin-bottom: 0.4em;
            }
            p {
                margin-top: 0;
                margin-bottom: 8px;
            }
            table {
                border-collapse: collapse;
                width: 100%;
                margin: 12px 0;
                display: block;
                overflow-x: auto;
                font-size: 13px;
            }
            th, td {
                border: 1px solid #dfe2e5;
                padding: 6px 10px;
                text-align: left;
            }
            th {
                background-color: #f6f8fa;
                font-weight: 600;
            }
            tr:nth-child(2n) {
                background-color: #f8f8f8;
            }
            tr:hover {
                background-color: #f1f8ff;
            }
            blockquote {
                color: #6a737d;
                border-left: 0.25em solid #dfe2e5;
                padding: 0 1em;
                margin: 0 0 10px 0;
            }
            code {
                padding: 0.2em 0.4em;
                margin: 0;
                font-size: 85%;
                background-color: rgba(27,31,35,0.05);
                border-radius: 3px;
                font-family: SFMono-Regular, Consolas, "Liberation Mono", Menlo, monospace;
            }
            pre {
                padding: 12px;
                overflow: auto;
                line-height: 1.45;
                background-color: #f6f8fa;
                border-radius: 3px;
                margin-bottom: 10px;
            }
            hr {
                height: 0.25em;
                padding: 0;
                margin: 16px 0;
                background-color: #e1e4e8;
                border: 0;
            }
            ul, ol {
                padding-left: 20px;
                margin-bottom: 10px;
            }
            li {
                margin: 2px 0;
            }
        """

    return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                {css_style}
            </style>
        </head>
        <body>
            {html_content}
        </body>
        </html>
        """


def markdown_to_plain_text(markdown_text: str) -> str:
    """
    will Markdown convertingasplain text
    
    remove Markdown formatmark，keepreadability
    """
    text = markdown_text
    
    # removetitlemark # ## ###
    text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)
    
    # removebold **text** -> text
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
    
    # removeslopebody *text* -> text
    text = re.sub(r'\*(.+?)\*', r'\1', text)
    
    # removereference > text -> text
    text = re.sub(r'^>\s+', '', text, flags=re.MULTILINE)
    
    # removelistmark - item -> item
    text = re.sub(r'^[-*]\s+', '• ', text, flags=re.MULTILINE)
    
    # removeseparateline ---
    text = re.sub(r'^---+$', '────────', text, flags=re.MULTILINE)
    
    # removetablesyntax |---|---|
    text = re.sub(r'\|[-:]+\|[-:|\s]+\|', '', text)
    text = re.sub(r'^\|(.+)\|$', r'\1', text, flags=re.MULTILINE)
    
    # cleanmultipleremainingemptyrow
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    return text.strip()


def _bytes(s: str) -> int:
    return len(s.encode('utf-8'))


def _chunk_by_max_bytes(content: str, max_bytes: int) -> List[str]:
    if _bytes(content) <= max_bytes:
        return [content]
    if max_bytes < MIN_MAX_BYTES:
        raise ValueError(f"max_bytes={max_bytes} < {MIN_MAX_BYTES}, may fall into infinite recursion。")
    
    sections: List[str] = []
    suffix = TRUNCATION_SUFFIX
    effective_max_bytes = max_bytes - _bytes(suffix)
    if effective_max_bytes <= 0:
        effective_max_bytes = max_bytes
        suffix = ""
        
    while True:
        chunk, content = slice_at_max_bytes(content, effective_max_bytes)
        if content.strip() != "":
            sections.append(chunk + suffix)
        else:
            # mostaftera paragraph，directlyaddandleaveloop
            sections.append(chunk)
            break
    return sections


def chunk_content_by_max_bytes(content: str, max_bytes: int, add_page_marker: bool = False) -> List[str]:
    """
    bybytescountintelligentsplittingmessagecontent
    
    Args:
        content: completemessagecontent
        max_bytes: single entrymessagemax bytes
        add_page_marker: whetheraddpaginationmark
        
    Returns:
        splittingblock afterlist
    """
    def _chunk(content: str, max_bytes: int) -> List[str]:
        # prioritize byseparateline/titlesplitting，guaranteepaginationnatural
        if max_bytes < MIN_MAX_BYTES:
            raise ValueError(f"max_bytes={max_bytes} < {MIN_MAX_BYTES}, may fall into infinite recursion。")
        
        if _bytes(content) <= max_bytes:
            return [content]
        
        sections, separator = _chunk_by_separators(content)
        if separator == "" and len(sections) == 1:
            # unable tointelligentsplitting，thenmandatoryby word countsplitting
            return _chunk_by_max_bytes(content, max_bytes)
        
        chunks: List[str] = []
        current_chunk: List[str] = []
        current_bytes = 0
        separator_bytes = _bytes(separator) if separator else 0
        effective_max_bytes = max_bytes - separator_bytes

        for section in sections:
            section += separator
            section_bytes = _bytes(section)
            
            # ifsingle section thenextra long，needmandatorytruncate
            if section_bytes > effective_max_bytes:
                # firstsavingcurrentaccumulatecontent
                if current_chunk:
                    chunks.append("".join(current_chunk))
                    current_chunk = []
                    current_bytes = 0

                # mandatorybybytestruncate，avoidentire segmentbytruncatelost
                section_chunks = _chunk(
                    section[:-separator_bytes], effective_max_bytes
                )
                section_chunks[-1] = section_chunks[-1] + separator
                chunks.extend(section_chunks)
                continue

            # checkaddafterwhetherextra long
            if current_bytes + section_bytes > effective_max_bytes:
                # savingcurrent block，startingnewblock
                if current_chunk:
                    chunks.append("".join(current_chunk))
                current_chunk = [section]
                current_bytes = section_bytes
            else:
                current_chunk.append(section)
                current_bytes += section_bytes
                
        # addmostafteroneblock
        if current_chunk:
            chunks.append("".join(current_chunk))
            
        # removemostafteronecountblocksplittingsymbol
        if (chunks and 
            len(chunks[-1]) > separator_bytes and 
            chunks[-1][-separator_bytes:] == separator
        ):
            chunks[-1] = chunks[-1][:-separator_bytes]
        
        return chunks
    
    if add_page_marker:
        max_bytes = max_bytes - PAGE_MARKER_SAFE_BYTES
    
    chunks = _chunk(content, max_bytes)
    if add_page_marker:
        total_chunks = len(chunks)
        for i, chunk in enumerate(chunks):
            chunks[i] = chunk + _page_marker(i, total_chunks)
    return chunks


def slice_at_max_bytes(text: str, max_bytes: int) -> tuple[str, str]:
    """
    bybytescounttruncatestring，ensurenotwillinmultiplebytescharacterinbetweentruncate

    Args:
        text: needtruncatestring
        max_bytes: max bytes

    Returns:
        (truncateafterstring, remainingnottruncatecontent)
    """
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text, ""

    # frommax bytesstartingtobeforefind，findtocomplete UTF-8 characterboundary
    truncated = encoded[:max_bytes]
    while truncated and (truncated[-1] & 0xC0) == 0x80:
        truncated = truncated[:-1]

    truncated = truncated.decode('utf-8', errors='ignore')
    return truncated, text[len(truncated):]


def format_feishu_markdown(content: str) -> str:
    """
    willgeneric Markdown convertingasFeishu lark_md morefriendlygoodformat
    
    convertingrule：
    - Feishunot supported Markdown title（# / ## / ###），useboldreplace
    - referenceblockuseprefixreplace
    - separatelineunifiedasdetailline
    - tableconvertingasitemsitemlist
    
    Args:
        content: raw Markdown content
        
    Returns:
        convertingafterFeishu Markdown formatcontent
        
    Example:
        >>> markdown = "# title\\n> reference\\n| column1 | column2 |"
        >>> formatted = format_feishu_markdown(markdown)
        >>> print(formatted)
        **title**
        💬 reference
        • column1：value1 | column2：value2
    """
    def _flush_table_rows(buffer: List[str], output: List[str]) -> None:
        """willtable bufferzoneinrowconvertingasFeishuformat"""
        if not buffer:
            return

        def _parse_row(row: str) -> List[str]:
            """parsingtablerow，extractunitstyle"""
            cells = [c.strip() for c in row.strip().strip('|').split('|')]
            return [c for c in cells if c]

        rows = []
        for raw in buffer:
            # skipseparaterow（e.g. |---|---|）
            if re.match(r'^\s*\|?\s*[:-]+\s*(\|\s*[:-]+\s*)+\|?\s*$', raw):
                continue
            parsed = _parse_row(raw)
            if parsed:
                rows.append(parsed)

        if not rows:
            return

        header = rows[0]
        data_rows = rows[1:] if len(rows) > 1 else []
        for row in data_rows:
            pairs = []
            for idx, cell in enumerate(row):
                key = header[idx] if idx < len(header) else f"column{idx + 1}"
                pairs.append(f"{key}：{cell}")
            output.append(f"• {' | '.join(pairs)}")

    lines = []
    table_buffer: List[str] = []

    for raw_line in content.splitlines():
        line = raw_line.rstrip()

        # processingtablerow
        if line.strip().startswith('|'):
            table_buffer.append(line)
            continue

        # refreshtable bufferzone
        if table_buffer:
            _flush_table_rows(table_buffer, lines)
            table_buffer = []

        # convertingtitle（# ## ### etc）
        if re.match(r'^#{1,6}\s+', line):
            title = re.sub(r'^#{1,6}\s+', '', line).strip()
            line = f"**{title}**" if title else ""
        # convertingreferenceblock
        elif line.startswith('> '):
            quote = line[2:].strip()
            line = f"💬 {quote}" if quote else ""
        # convertingseparateline
        elif line.strip() == '---':
            line = '────────'
        # convertinglistitem
        elif line.startswith('- '):
            line = f"• {line[2:].strip()}"

        lines.append(line)

    # processingendtailtable
    if table_buffer:
        _flush_table_rows(table_buffer, lines)

    return "\n".join(lines).strip()


def _chunk_by_separators(content: str) -> tuple[list[str], str]:
    """
    viasplittinglineetcspecialcharacterwillmessagecontentsplittingasmultiplecountzoneblock
    
    Args:
        content: completemessagecontent
        
    Returns:
        sections: splittingblock afterlist
        separator: zoneblockbetweenseparatesymbol，None indicatesunable tosplitting
    """
    # intelligentsplitting：prioritize by "---" separate（stockbetweenseparateline）
    # itstimestryeachleveltitlesplitting
    if "\n---\n" in content:
        sections = content.split("\n---\n")
        separator = "\n---\n"
    elif "\n# " in content:
        # by # splitting (compatiblefirst leveltitle)
        parts = content.split("\n## ")
        sections = [parts[0]] + [f"## {p}" for p in parts[1:]]
        separator = "\n"
    elif "\n## " in content:
        # by ## splitting (compatibletwoleveltitle)
        parts = content.split("\n## ")
        sections = [parts[0]] + [f"## {p}" for p in parts[1:]]
        separator = "\n"
    elif "\n### " in content:
        # by ### splitting
        parts = content.split("\n### ")
        sections = [parts[0]] + [f"### {p}" for p in parts[1:]]
        separator = "\n"
    elif "\n**" in content:
        # by ** boldtitlesplitting (compatible AI notoutputstandard Markdown titlesituation)
        parts = content.split("\n**")
        sections = [parts[0]] + [f"**{p}" for p in parts[1:]]
        separator = "\n"
    elif "\n" in content:
        # by \n splitting
        sections = content.split("\n")
        separator = "\n"
    else:
        return [content], ""
    return sections, separator


def _chunk_by_max_words(content: str, max_words: int, special_char_len: int = 2) -> list[str]:
    """
    by word countsplittingmessagecontent
    
    Args:
        content: completemessagecontent
        max_words: single entrymessagemaxcharactercount
        special_char_len: length per special character，defaultas 2
        
    Returns:
        splittingblock afterlist
    """
    if _effective_len(content, special_char_len) <= max_words:
        return [content]
    if max_words < MIN_MAX_WORDS:
        raise ValueError(
            f"max_words={max_words} < {MIN_MAX_WORDS}, may fall into infinite recursion。"
        )

    sections = []
    suffix = TRUNCATION_SUFFIX
    effective_max_words = max_words - len(suffix)  # reservesuffix，avoidboundaryover limit
    if effective_max_words <= 0:
        effective_max_words = max_words
        suffix = ""

    while True:
        chunk, content = _slice_at_effective_len(content, effective_max_words, special_char_len)
        if content.strip() != "":
            sections.append(chunk + suffix)
        else:
            # mostaftera paragraph，directlyaddandleaveloop
            sections.append(chunk)
            break
    return sections


def chunk_content_by_max_words(
    content: str, 
    max_words: int, 
    special_char_len: int = 2,
    add_page_marker: bool = False
    ) -> list[str]:
    """
    by word countintelligentsplittingmessagecontent
    
    Args:
        content: completemessagecontent
        max_words: single entrymessagemaxcharactercount
        special_char_len: length per special character，defaultas 2
        add_page_marker: whetheraddpaginationmark
        
    Returns:
        splittingblock afterlist
    """
    def _chunk(content: str, max_words: int, special_char_len: int = 2) -> list[str]:
        if max_words < MIN_MAX_WORDS:
            # Safe guard，avoidnolimitrecursive
            # reasonin theory，max_wordsineach timerecursiveincanreducesmalltonolimitsmall，butactualinnottoopossiblysendgenerate，
            # exceptnon-each time_chunk_by_separatorsallcansuccessfulreturnseparatesymbol，andmax_wordsinitialvaluetoosmall。
            raise ValueError(f"max_words={max_words} < {MIN_MAX_WORDS}, may fall into infinite recursion。")
        
        if _effective_len(content, special_char_len) <= max_words:
            return [content]

        sections, separator = _chunk_by_separators(content)
        if separator == "" and len(sections) == 1:
            # unable tointelligentsplitting，thenmandatoryby word countsplitting
            return _chunk_by_max_words(content, max_words, special_char_len)

        chunks = []
        current_chunk = []
        current_word_len = 0
        separator_len = len(separator) if separator else 0
        effective_max_words = max_words - separator_len # reservesplittingsymbollength，avoidboundaryover limit

        for section in sections:
            section += separator
            section_word_len = _effective_len(section, special_char_len)

            # ifsingle section thenextra long，needmandatorytruncate
            if section_word_len > max_words:
                # firstsavingcurrentaccumulatecontent
                if current_chunk:
                    chunks.append("".join(current_chunk))

                # mandatorytruncatethiscountextra long section
                section_chunks = _chunk(
                    section[:-separator_len], effective_max_words, special_char_len
                    )
                section_chunks[-1] = section_chunks[-1] + separator
                chunks.extend(section_chunks)
                continue

            # checkaddafterwhetherextra long
            if current_word_len + section_word_len > max_words:
                # savingcurrent block，startingnewblock
                if current_chunk:
                    chunks.append("".join(current_chunk))
                current_chunk = [section]
                current_word_len = section_word_len
            else:
                current_chunk.append(section)
                current_word_len += section_word_len

        # addmostafteroneblock
        if current_chunk:
            chunks.append("".join(current_chunk))

        # removemostafteronecountblocksplittingsymbol
        if (chunks and
            len(chunks[-1]) > separator_len and
            chunks[-1][-separator_len:] == separator
        ):
            chunks[-1] = chunks[-1][:-separator_len]
        return chunks
    
    
    if add_page_marker:
        max_words = max_words - PAGE_MARKER_SAFE_LEN
    
    chunks = _chunk(content, max_words, special_char_len)
    if add_page_marker:
        total_chunks = len(chunks)
        for i, chunk in enumerate(chunks):
            chunks[i] = chunk + _page_marker(i, total_chunks)
    return chunks
