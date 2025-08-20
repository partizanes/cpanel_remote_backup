class CpanelAccount:
    def __init__(self, user, partition, suspended, uid):
        self.user      = user
        self.partition = partition
        self.suspended = suspended
        self.uid       = uid