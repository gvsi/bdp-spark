class Event:
    def __init__(self, event_type, event_subtype, event_timestamp):
        self.event_type = event_type
        self.event_subtype = event_subtype
        self.event_timestamp = event_timestamp

    @property
    def event_type(self):
        return self.event_type

    @property
    def event_subtype(self):
        return self.event_subtype

    @property
    def event_timestamp(self):
        return self.event_timestamp

    def __str__(self):
        return "<%s:%s,%s>" % (self.event_type, self.event_subtype, self.event_timestamp)

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                getattr(other, 'event_type', None) == self.event_type and
                getattr(other, 'event_subtype', None) == self.event_subtype and
                getattr(other, 'event_timestamp', None) == self.event_timestamp)

    def __hash__(self):
        return hash(self.event_type + self.event_subtype + self.event_timestamp)
