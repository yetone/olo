class MigrationVersion(object):
    def __init__(self, from_version, to_version):
        self.from_version = from_version
        self.to_version = to_version


def migration_db_field(model_class, from_version, to_version, delete=False):
    objs = model_class.gets_by()
    for obj in objs:
        for attr_name in model_class.__db_fields__:
            obj._data.pop(attr_name, None)
            obj._options.db_field_version = from_version
            v = getattr(obj, attr_name)
            obj._options.db_field_version = to_version
            if v is None:
                if delete:
                    delattr(obj, attr_name)
                # noqa not cover: https://bitbucket.org/ned/coveragepy/issues/198/continue-marked-as-not-covered
                continue  # pragma: no cover
            obj._data.pop(attr_name, None)
            setattr(obj, attr_name, v)
        obj.save()
