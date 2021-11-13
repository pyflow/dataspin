
class CQSStreamProvider:
    def __init__(self, key_pair, name):
        self.key_pair = key_pair
        self.name = name

    def get(self):
        pass

class COSStorageProvider:
    def __init__(self, key_pair):
        self.key_pair = key_pair
    
    def save(self, path, local_path):
        pass