import os
from lib.markdown import append_note


def test_creates_file_and_writes_entry(tmp_path):
    f = tmp_path / "notes.md"
    append_note(str(f), "My Note")
    text = f.read_text()
    assert "## My Note" in text
    assert "---" in text


def test_appends_to_existing_file(tmp_path):
    f = tmp_path / "notes.md"
    f.write_text("# Existing\n")
    append_note(str(f), "Second Note")
    text = f.read_text()
    assert "# Existing" in text
    assert "## Second Note" in text


def test_tags_formatted(tmp_path):
    f = tmp_path / "notes.md"
    append_note(str(f), "Tagged", tags=["kafka", "devops"])
    text = f.read_text()
    assert "Tags: #kafka, #devops" in text


def test_no_tags_omits_tag_line(tmp_path):
    f = tmp_path / "notes.md"
    append_note(str(f), "No Tags")
    assert "Tags:" not in f.read_text()


def test_content_included(tmp_path):
    f = tmp_path / "notes.md"
    append_note(str(f), "Title", content="Some body text")
    text = f.read_text()
    assert "Some body text" in text


def test_duplicate_content_skipped(tmp_path):
    """When content == title, Google Tasks duplication — content should be omitted."""
    f = tmp_path / "notes.md"
    append_note(str(f), "Same Text", content="Same Text")
    text = f.read_text()
    # Title appears once (as heading), content not repeated in body
    assert text.count("Same Text") == 1


def test_custom_timestamp(tmp_path):
    f = tmp_path / "notes.md"
    append_note(str(f), "Timed", timestamp="2026-03-24 10:00")
    assert "*2026-03-24 10:00*" in f.read_text()


def test_default_timestamp_present(tmp_path):
    f = tmp_path / "notes.md"
    append_note(str(f), "Auto Time")
    text = f.read_text()
    # Should have a timestamp line with *...*
    assert text.count("*") >= 2
