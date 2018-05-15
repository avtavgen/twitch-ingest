import helpers


def batches(iterable, n=10):
    """divide a single list into a list of lists of size n"""
    batchLen = len(iterable)
    for ndx in range(0, batchLen, n):
        yield list(iterable[ndx:min(ndx + n, batchLen)])


class SocialStatements:

    def __init__(self, engine):
        self.users = []
        self.engine = engine

    user_schema = {
        "table_name": "twitch_user_info",
        "options": {
            "primary_key": ["user_id"],
        },
        "columns": {
            "user_id": "text",
            "name": "text",
            "views": "bigint",
            "description": "text",
            "url": "text",
            "followers": "int",
            "videos": "int"
        }
    }

    def save(self, logging_name='social ingest', batch_size=50, users=None):
        """Write these social statements to the data engine in the appropriate manner."""
        logger = helpers.get_logger(logging_name)
        self.users = users
        if self.users:
            logger.info('about to send {} user statements to the data engine'.format(len(self.users)))
            self._write_batches(self.engine, logger, self.user_schema, self.users, batch_size)
        else:
            logger.debug('skipping user ingest, no records in these social statements')

    @staticmethod
    def _write_batches(engine, logger, schema, data, batch_size=40):
        for rows in batches(data, batch_size):
            # logger.info('Rows: {}'.format(rows))
            res = engine.save(schema, list(rows)).result()
            logger.info(res)
