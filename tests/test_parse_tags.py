from lib.google_tasks import parse_tags


def test_single_tag():
    clean, category, tags = parse_tags("Learn Kafka #kafka")
    assert clean == "Learn Kafka"
    assert category == "kafka"
    assert tags == ["kafka"]


def test_multiple_tags():
    clean, category, tags = parse_tags("Note #kafka #devops")
    assert clean == "Note"
    assert category == "kafka"  # first tag becomes category
    assert tags == ["kafka", "devops"]


def test_no_tags():
    clean, category, tags = parse_tags("Plain note")
    assert clean == "Plain note"
    assert category is None
    assert tags == []


def test_tags_lowercased():
    _, category, tags = parse_tags("Title #Kafka #DevOps")
    assert category == "kafka"
    assert tags == ["kafka", "devops"]


def test_only_tags():
    clean, category, tags = parse_tags("#kafka #devops")
    assert clean == ""
    assert category == "kafka"


def test_tag_in_middle():
    clean, _, tags = parse_tags("Learn #kafka basics")
    assert clean == "Learn basics"
    assert tags == ["kafka"]
