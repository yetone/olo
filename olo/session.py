class QuerySession(object):

    def __init__(self):
        self.entities = []

    def add_entity(self, entity, idx=None):
        if idx is None:
            idx = len(self.entities)

        if hasattr(entity, '_olo_qs'):
            entity._olo_qs = self
            entity._olo_qs_idx = idx

        self.entities.append(entity)
