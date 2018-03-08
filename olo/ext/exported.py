IS_EXPORTED_PROPERTY = '_olo_is_exported_property'


class exported_property(property):

    def __init__(self, func):
        super(exported_property, self).__init__(func)
        setattr(self, IS_EXPORTED_PROPERTY, True)
