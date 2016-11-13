class AWSHelper(object):
    def __init__(self, session):
        self.session = session
        pass

    def get_generic_client(self, service_name, globals):
        client = self.get_client(service_name, globals.region,
                                 globals.endpoint_url, globals.verify_ssl)
        return client

    def get_client(self, service_name, region, endpoint_url, verify_ssl):
        client = self.session.create_client(
            service_name,
            region_name=region,
            endpoint_url=endpoint_url,
            verify=verify_ssl)
        return client
