from mongosync.oplog_sync import start_sync_oplog
from mongosync import conf
import click
import os


def resolution(string=""):
    for s in string.split(","):
        if s == "":
            continue
        r = s.split("=", 1)
        if len(r) == 2:
            yield r
        else:
            yield r[0], r[0]


@click.command("oplog")
@click.option("-s", "--source", default=conf.SOURCE)
@click.option("-t", "--target", default=conf.TARGET)
@click.option("-m", "--db_map", default=conf.DB_MAP)
@click.option("-a", "--await", default=conf.AWAIT, type=click.INT)
@click.option("-t", "--ts_file", default=conf.TS_FILE)
@click.option("--start", default=None)
@click.option("--end", default=None)
@click.argument("conf", default=None, required=False)
def oplog(conf, **kwargs):
    if conf is not None and os.path.isfile(conf):
        import json
        config = json.load(open(conf))
        kwargs.update({key.lower(): value for key, value in config.items()})
    db_map = kwargs["db_map"]
    if isinstance(db_map, str):
        kwargs["db_map"] = dict(resolution(db_map))
    start_sync_oplog(**kwargs)


group = click.Group("mongosync",
                    {"oplog": oplog})


if __name__ == '__main__':
    group()