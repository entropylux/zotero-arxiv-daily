"""Local delivery history and protection against uncertain SMTP outcomes."""
from datetime import datetime
import json
from pathlib import Path
import re
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    temporary.replace(path)


def paper_key(paper):
    if paper.source == 'arxiv':
        return 'arxiv:' + re.sub(r'v\d+$', '', urlsplit(paper.url).path.removeprefix('/abs/'))
    return paper.source + ':' + paper.url


class DeliveryState:
    def __init__(self, directory=None):
        self.path = Path(directory) / 'delivery.json' if directory else None
        self.state = {'papers': {}, 'pending': None}
        if self.path and self.path.exists():
            self.state = json.loads(self.path.read_text(encoding='utf-8'))
        if not isinstance(self.state.get('papers'), dict) or 'pending' not in self.state:
            raise ValueError('Invalid delivery history; resolve before sending')
        # Carry forward confirmed deliveries and any unresolved old SMTP attempt.
        legacy = Path(directory) / 'curation/delivery.json' if directory else None
        self.legacy_pending = None
        if legacy and legacy.exists():
            old = json.loads(legacy.read_text(encoding='utf-8'))
            self.legacy_pending = old['pending']
            for key, record in old['papers'].items():
                self.state['papers'].setdefault('arxiv:' + key, record)

    def check_pending(self):
        if self.state['pending'] or self.legacy_pending:
            raise RuntimeError('Previous email delivery is uncertain; no automatic resend')

    def unseen(self, papers):
        seen = set(self.state['papers'])
        output = []
        for paper in papers:
            key = paper_key(paper)
            if key not in seen:
                output.append(paper)
                seen.add(key)
        return output

    def send(self, papers, callback):
        self.check_pending()
        today = datetime.now(ZoneInfo('Asia/Shanghai')).date().isoformat()
        self.state['pending'] = {'date': today, 'papers': [paper_key(p) for p in papers]}
        if self.path:
            write_json(self.path, self.state)
        callback()
        for paper in papers:
            self.state['papers'][paper_key(paper)] = {'date': today, 'title': paper.title}
        self.state['pending'] = None
        if self.path:
            write_json(self.path, self.state)
