import pytest
import requests
import uuid
import hashlib
import os


class TestHttpApi:
    endpoint = "http://localhost:33087"
    payload = '''
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Ut blandit rhoncus magna eget sollicitudin. Ut sem nisi, molestie quis neque ut, pellentesque sollicitudin velit. Quisque consequat risus erat, vitae volutpat est faucibus posuere. Nunc ultricies turpis non ipsum consectetur tincidunt sit amet sed urna. Donec fermentum ut elit ut auctor. Morbi et semper nulla, quis rhoncus massa. Curabitur consequat lorem sapien, vehicula tincidunt metus viverra nec. Nam consequat orci justo, ac malesuada diam scelerisque sed. Phasellus ut ligula purus. Ut elementum odio eget nunc euismod, eu pharetra eros efficitur. Donec faucibus massa eget mi pellentesque, bibendum sollicitudin elit efficitur. Nunc ante lacus, viverra vitae congue aliquet, molestie ut risus.
        Vivamus viverra molestie nulla, vel maximus odio vestibulum vitae. Quisque tempus blandit mi, a fringilla leo. In quis nulla quis quam fringilla tincidunt. Nam sit amet elit eget diam molestie mattis in sed ipsum. Sed tincidunt, odio at feugiat placerat, purus nisi tincidunt dolor, vitae ornare lorem nisl eget enim. Praesent neque dolor, tincidunt eget justo et, ullamcorper consectetur ligula. Ut vulputate arcu erat, in sagittis ligula cursus ac. Etiam vel cursus leo, vehicula tempus tortor. Donec ipsum risus, interdum id iaculis eget, condimentum a urna.
        Maecenas rhoncus euismod leo a dignissim. Nulla eu pellentesque orci, sed viverra odio. Mauris vulputate sapien ut consectetur consectetur. Vestibulum luctus lorem vitae ligula ornare, vitae congue nibh tempus. Suspendisse et aliquet felis. Proin eu sollicitudin purus. Aliquam erat volutpat. Donec porta massa a ex pretium lobortis. Ut cursus ac velit ut ultricies. Pellentesque porttitor tempor venenatis.
        Mauris vitae pellentesque odio, in varius orci. Fusce purus dui, tristique sed cursus id, consectetur et nisl. Ut finibus rhoncus mauris, at varius metus pretium quis. Interdum et malesuada fames ac ante ipsum primis in faucibus. Curabitur convallis ac nulla eget tempor. Suspendisse hendrerit nisi imperdiet mauris pellentesque ullamcorper. Nullam molestie rutrum mollis. Nunc pellentesque enim elit, sit amet aliquam ipsum venenatis quis. Proin pharetra arcu et bibendum vulputate. Fusce ac luctus sapien.
        Sed mattis tellus leo. Donec at vestibulum erat. Ut a lobortis nisi. Nunc volutpat, velit a iaculis dignissim, nulla tellus porttitor justo, et lobortis orci lorem a urna. Donec sit amet vulputate neque, nec aliquam lorem. Curabitur non nisi enim. Vestibulum lectus nulla, suscipit quis pretium ac, feugiat vulputate ante. Morbi rutrum vestibulum mi. Fusce semper rutrum mauris. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Nam ut erat ac nibh lacinia gravida. Donec convallis, massa vel facilisis vehicula, elit tortor lacinia libero, sit amet varius nibh enim a lorem. Suspendisse tincidunt elementum justo.
        Aliquam scelerisque metus ante. Nulla tempus quam diam, in consequat ex semper ut. Integer viverra urna odio, quis mattis metus elementum ac. Nullam id leo non dui fermentum pulvinar vel eu massa. Nunc porta tempor turpis vel ultrices. In ac arcu eu dui gravida euismod. Aenean pellentesque maximus magna, eget facilisis nisi vestibulum eget. Praesent laoreet velit eget rhoncus ullamcorper. Nulla facilisi. Morbi aliquet quam ut egestas convallis. Ut vel lorem id neque ultrices tincidunt in.
    '''

    def test_index(self):
        r = requests.get(self.endpoint)
        assert 200 == r.status_code
        assert "The little block engine that could!" == r.text

    def test_status(self):
        url = self.endpoint + "/status"
        r = requests.get(url)
        assert 200 == r.status_code
        r = r.json()
        assert "normal" == r["node"]["status"]

    def test_metrics(self):
        url = self.endpoint + "/metrics"
        r = requests.get(url)
        assert 200 == r.status_code
        assert -1 != r.text.find("http_requests_total")

    def test_put_get(self):
        # payload = "\x00"+os.urandom(4*1024*1024)+"\x00"
        object_id = str(uuid.uuid4())
        url = self.endpoint + "/block/" + object_id
        r = requests.put(
            url,
            data=self.payload,
            headers={
                'content-type': 'text/plain',
                'v-hash-fun': '0',
                'v-hash': hashlib.md5(self.payload).hexdigest(),
                'v-compress': 'lz4',
            }
        )
        assert 204 == r.status_code

        r = requests.get(url)
        assert 200 == r.status_code
        assert self.payload == r.text

    def test_etag(self):
        object_id = str(uuid.uuid4())
        url = self.endpoint + "/block/" + object_id
        r = requests.put(
            url,
            data=self.payload,
            headers={
                'content-type': 'text/plain',
                'v-hash-fun': '0',
                'v-hash': hashlib.md5(self.payload).hexdigest(),
                'v-compress': 'lz4',
            }
        )
        assert 204 == r.status_code

        r = requests.get(url)
        etag = r.headers["etag"]
        assert etag != ""

        r = requests.get(
            url,
            headers={
                'if-none-match': etag
            }
        )
        assert 304 == r.status_code

    def test_delete(self):
        object_id = str(uuid.uuid4())
        url = self.endpoint + "/block/" + object_id
        r = requests.put(
            url,
            data=self.payload,
        )
        assert 204 == r.status_code

        r = requests.get(url)
        assert 200 == r.status_code
        assert self.payload == r.text

        r = requests.delete(url)
        assert 204 == r.status_code

        r = requests.get(url)
        assert 404 == r.status_code

    def test_crud_stats(self):
        status_url = self.endpoint + "/status"
        r = requests.get(status_url)
        assert 200 == r.status_code
        init_status = r.json()['storage']

        object_id = str(uuid.uuid4())
        object_url = self.endpoint + "/block/" + object_id
        r = requests.put(
            object_url,
            data=self.payload,
        )
        assert 204 == r.status_code

        r = requests.get(status_url)
        assert 200 == r.status_code
        status_after_put = r.json()['storage']

        assert init_status['init_bytes'] == status_after_put['init_bytes']
        assert init_status['active_slots'] == status_after_put['active_slots']
        assert init_status['gc_bytes'] == status_after_put['gc_bytes']
        assert init_status['move_bytes'] == status_after_put['move_bytes']
        assert init_status['objects'] + 1 == status_after_put['objects']
        assert init_status['avail_bytes'] - len(self.payload) == status_after_put['avail_bytes']

        r = requests.delete(object_url)
        assert 204 == r.status_code

        r = requests.get(status_url)
        assert 200 == r.status_code
        status_after_delete = r.json()['storage']

        assert status_after_put['init_bytes'] == status_after_delete['init_bytes']
        assert status_after_put['active_slots'] == status_after_delete['active_slots']
        assert status_after_put['gc_bytes'] + len(self.payload) == status_after_delete['gc_bytes']
        assert status_after_put['move_bytes'] == status_after_delete['move_bytes']
        assert status_after_put['objects'] - 1 == status_after_delete['objects']
        assert status_after_put['avail_bytes'] == status_after_delete['avail_bytes']

    def test_uniq_put(self):
        object_id = "put_id"
        object_url = self.endpoint + "/block/" + object_id

        requests.delete(object_url)

        ## first write
        r = requests.put(
            object_url,
            data=self.payload,
        )
        assert 204 == r.status_code

        ## second write
        r = requests.put(
            object_url,
            data=self.payload,
        )
        assert 302 == r.status_code

        ## update with post
        r = requests.post(
            object_url,
            data=self.payload,
        )
        assert 204 == r.status_code

    def test_write_without_id(self):
        object_url = self.endpoint + "/block"

        r = requests.put(
            object_url,
            data=self.payload,
        )
        assert 200 == r.status_code
        assert 32 == len(r.text)