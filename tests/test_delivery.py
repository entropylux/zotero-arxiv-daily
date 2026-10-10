import pytest
from tests.canned_responses import make_sample_paper
from zotero_arxiv_daily.delivery import DeliveryState, write_json


def test_history_and_version_dedup(tmp_path):
    paper = make_sample_paper(url='https://arxiv.org/abs/2610.12345v1')
    revised = make_sample_paper(url='https://arxiv.org/abs/2610.12345v2')
    state = DeliveryState(tmp_path)
    assert state.unseen([paper, revised]) == [paper]
    assert not (tmp_path / 'delivery.json').exists()
    state.send([paper], lambda: None)
    assert DeliveryState(tmp_path).unseen([revised]) == []


def test_uncertain_send_blocks_retry(tmp_path):
    state = DeliveryState(tmp_path)
    def fail():
        raise TimeoutError()
    with pytest.raises(TimeoutError):
        state.send([make_sample_paper()], fail)
    with pytest.raises(RuntimeError, match='uncertain'):
        DeliveryState(tmp_path).check_pending()


def test_legacy_history_import_is_read_only(tmp_path):
    write_json(tmp_path / 'curation/delivery.json', {'papers': {'2610.12345': {}}, 'pending': None})
    state = DeliveryState(tmp_path)
    assert not state.unseen([make_sample_paper(url='https://arxiv.org/abs/2610.12345v2')])
    assert not (tmp_path / 'delivery.json').exists()
