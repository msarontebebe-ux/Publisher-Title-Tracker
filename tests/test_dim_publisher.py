"""
Publisher dimension: 17 raw publisher spellings map to 5 clean publishers by STREAM,
with the raw name kept as the imprint level. Raw data is never changed.
"""

from sqlalchemy import create_engine, text

from pipeline.config import load_publisher_streams, publisher_of
from pipeline.models import ScrapedAuthor, ScrapedPublication
from pipeline.runner import _write_events, _write_silver

LNCS = 'Lecture Notes in Computer Science'
CCIS = 'Communications in Computer and Information Science'


def rows(url, sql):
    engine = create_engine(url)
    with engine.connect() as conn:
        out = conn.execute(text(sql)).fetchall()
    engine.dispose()
    return out


def pub(doi, series, publisher):
    return ScrapedPublication(title=f'T {doi}', authors=[ScrapedAuthor(name='A')], year=2026,
                              series=series, publisher=publisher, doi=doi)


# ----------------------------------------------------------------------- the rule
def test_every_configured_stream_maps_to_its_publisher():
    streams = load_publisher_streams()
    for series in streams[0]['series']:                      # the six Springer series
        assert publisher_of(series, streams) == 'Springer'
    for name in ('Elsevier', 'Wiley', 'IEEE', 'ACM'):
        assert publisher_of(name, streams) == name


def test_an_unknown_stream_maps_to_itself_and_is_not_dropped():
    assert publisher_of('Some New Stream', load_publisher_streams()) == 'Some New Stream'


# -------------------------------------------------------------- the dimension tables
class TestDimension:

    def seed(self):
        records = [pub('10.1007/a', LNCS, 'Springer Nature Switzerland'),
                   pub('10.1007/b', LNCS, 'Springer-Verlag'),
                   pub('10.1007/c', CCIS, 'Springer Berlin Heidelberg'),
                   pub('10.1109/x', 'IEEE', 'IEEE'),
                   pub('10.1109/y', 'IEEE', 'Institute of Electrical and Electronics Engineers (IEEE)'),
                   pub('10.1002/w', 'Wiley', 'Wiley')]
        _write_silver(records, 'mixed')
        for r in records:
            _write_events([r], r.series)

    def refresh(self):
        from pipeline.db import DatabaseConfig
        from pipeline.gold import compute_dim_publisher
        session = DatabaseConfig.get_session()
        try:
            added = compute_dim_publisher(session)
            session.commit()
            return added
        finally:
            session.close()

    def test_spellings_collapse_to_publishers_with_the_raw_name_as_imprint(self, db_env):
        self.seed()
        self.refresh()
        assert rows(db_env, 'SELECT publisher, imprint FROM gold.dim_publisher ORDER BY 1, 2') == [
            ('IEEE', 'IEEE'),
            ('IEEE', 'Institute of Electrical and Electronics Engineers (IEEE)'),
            ('Springer', 'Springer Berlin Heidelberg'),
            ('Springer', 'Springer Nature Switzerland'),
            ('Springer', 'Springer-Verlag'),
            ('Wiley', 'Wiley')]
        assert rows(db_env, 'SELECT COUNT(DISTINCT publisher) FROM gold.dim_publisher') == [(3,)]

    def test_every_title_and_event_can_be_joined_to_a_publisher(self, db_env):
        self.seed()
        self.refresh()
        sql = ('SELECT d.publisher, COUNT(*) FROM {t} x JOIN gold.publisher_map m '
               'ON m.series = x.series AND m.raw_publisher = x.publisher '
               'JOIN gold.dim_publisher d ON d.publisher_key = m.publisher_key GROUP BY 1 ORDER BY 1')
        for table in ('publications', 'events'):
            assert rows(db_env, sql.format(t=table)) == [('IEEE', 2), ('Springer', 3), ('Wiley', 1)]

    def test_running_it_again_adds_nothing_and_a_new_spelling_is_picked_up(self, db_env):
        self.seed()
        assert self.refresh() == 6
        assert self.refresh() == 0
        new = pub('10.1007/z', LNCS, 'Springer Nature Germany')           # a spelling never seen before
        _write_silver([new], LNCS)
        assert self.refresh() == 1
        assert ('Springer', 'Springer Nature Germany') in rows(
            db_env, 'SELECT publisher, imprint FROM gold.dim_publisher')

    def test_the_same_raw_name_in_two_streams_is_kept_apart(self, db_env):
        _write_silver([pub('10.1/a', 'Wiley', 'Shared Name'), pub('10.1/b', 'ACM', 'Shared Name')], 'x')
        self.refresh()
        assert rows(db_env, 'SELECT series, d.publisher FROM gold.publisher_map m JOIN gold.dim_publisher d '
                            'USING (publisher_key) ORDER BY 1') == [('ACM', 'ACM'), ('Wiley', 'Wiley')]

    def test_raw_data_is_never_changed(self, db_env):
        self.seed()
        before = rows(db_env, 'SELECT publisher FROM publications ORDER BY doi')
        self.refresh()
        assert rows(db_env, 'SELECT publisher FROM publications ORDER BY doi') == before

    def test_refresh_gold_keeps_the_dimension_up_to_date(self, db_env):
        from pipeline.gold import refresh_gold
        self.seed()
        refresh_gold(series_filter=LNCS)
        # the star schema refresh adds the 'Unknown' member (key -1) on top of the 6 real imprints
        assert rows(db_env, 'SELECT COUNT(*) FROM gold.dim_publisher WHERE publisher_key > 0') == [(6,)]
