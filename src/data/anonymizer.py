# src/data/anonymizer.py
def create_anonymous_id(identifier, salt=None):
    """
    Generate deterministic anonymous ID
    
    Description:
    This function generates a deterministic anonymous ID based on the provided
    identifier and salt. If no salt is provided, a random one is generated.
    
    If a salt is provided and the same identifier is passed in, the same
    anonymous ID will be generated, yielding deterministically the same result 
    for a given identifier and salt each time.
    """
    import os
    import hashlib
    if salt is None:
        # Generate a random salt if none is provided
        salt = os.urandom(32)
    # Combine the identier and the sald, then hash the result
    return hashlib.sha256(f"{identifier}{salt}".encode()).hexdigest()[:12]

# Master mapping class
class IDMapper:
    def init(self, salt_file=None):
        self.mappings = {}
        self.salt = self._load_or_create_salt(salt_file)

    def add_identifier(self, original_id, source):
        if original_id not in self.mappings:
            anon_id = create_anonymous_id(original_id, self.salt)
            self.mappings[original_id] = {
                'anonymous_id': anon_id,
                'source': source
            }
        return self.mappings[original_id]['anonymous_id']

    def save_mapping(self, output_dir):
        # Save separate files for each source
        for source in ['turnstile', 'trust', 'survey']:
            source_map = {k: v['anonymous_id'] 
                         for k,v in self.mappings.items() 
                         if v['source'] == source}
            pd.DataFrame(source_map.items(),
                        columns=['original_id', 'anonymous_id'])\
              .to_csv(f"{output_dir}/{source}_id_map.csv")