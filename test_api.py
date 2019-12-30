import pytest
import requests
import uuid
import hashlib
import os


class TestHttpApi:
    endpoint = "http://localhost:33087"

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
        payload = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Ut blandit rhoncus magna eget sollicitudin. Ut sem nisi, molestie quis neque ut, pellentesque sollicitudin velit. Quisque consequat risus erat, vitae volutpat est faucibus posuere. Nunc ultricies turpis non ipsum consectetur tincidunt sit amet sed urna. Donec fermentum ut elit ut auctor. Morbi et semper nulla, quis rhoncus massa. Curabitur consequat lorem sapien, vehicula tincidunt metus viverra nec. Nam consequat orci justo, ac malesuada diam scelerisque sed. Phasellus ut ligula purus. Ut elementum odio eget nunc euismod, eu pharetra eros efficitur. Donec faucibus massa eget mi pellentesque, bibendum sollicitudin elit efficitur. Nunc ante lacus, viverra vitae congue aliquet, molestie ut risus."
        payload += "Vivamus viverra molestie nulla, vel maximus odio vestibulum vitae. Quisque tempus blandit mi, a fringilla leo. In quis nulla quis quam fringilla tincidunt. Nam sit amet elit eget diam molestie mattis in sed ipsum. Sed tincidunt, odio at feugiat placerat, purus nisi tincidunt dolor, vitae ornare lorem nisl eget enim. Praesent neque dolor, tincidunt eget justo et, ullamcorper consectetur ligula. Ut vulputate arcu erat, in sagittis ligula cursus ac. Etiam vel cursus leo, vehicula tempus tortor. Donec ipsum risus, interdum id iaculis eget, condimentum a urna."
        payload += "Maecenas rhoncus euismod leo a dignissim. Nulla eu pellentesque orci, sed viverra odio. Mauris vulputate sapien ut consectetur consectetur. Vestibulum luctus lorem vitae ligula ornare, vitae congue nibh tempus. Suspendisse et aliquet felis. Proin eu sollicitudin purus. Aliquam erat volutpat. Donec porta massa a ex pretium lobortis. Ut cursus ac velit ut ultricies. Pellentesque porttitor tempor venenatis."
        payload += "Mauris vitae pellentesque odio, in varius orci. Fusce purus dui, tristique sed cursus id, consectetur et nisl. Ut finibus rhoncus mauris, at varius metus pretium quis. Interdum et malesuada fames ac ante ipsum primis in faucibus. Curabitur convallis ac nulla eget tempor. Suspendisse hendrerit nisi imperdiet mauris pellentesque ullamcorper. Nullam molestie rutrum mollis. Nunc pellentesque enim elit, sit amet aliquam ipsum venenatis quis. Proin pharetra arcu et bibendum vulputate. Fusce ac luctus sapien."
        payload += "Sed mattis tellus leo. Donec at vestibulum erat. Ut a lobortis nisi. Nunc volutpat, velit a iaculis dignissim, nulla tellus porttitor justo, et lobortis orci lorem a urna. Donec sit amet vulputate neque, nec aliquam lorem. Curabitur non nisi enim. Vestibulum lectus nulla, suscipit quis pretium ac, feugiat vulputate ante. Morbi rutrum vestibulum mi. Fusce semper rutrum mauris. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Nam ut erat ac nibh lacinia gravida. Donec convallis, massa vel facilisis vehicula, elit tortor lacinia libero, sit amet varius nibh enim a lorem. Suspendisse tincidunt elementum justo."
        payload += "Aliquam scelerisque metus ante. Nulla tempus quam diam, in consequat ex semper ut. Integer viverra urna odio, quis mattis metus elementum ac. Nullam id leo non dui fermentum pulvinar vel eu massa. Nunc porta tempor turpis vel ultrices. In ac arcu eu dui gravida euismod. Aenean pellentesque maximus magna, eget facilisis nisi vestibulum eget. Praesent laoreet velit eget rhoncus ullamcorper. Nulla facilisi. Morbi aliquet quam ut egestas convallis. Ut vel lorem id neque ultrices tincidunt in."
        object_id = str(uuid.uuid4())
        url = self.endpoint + "/block/" + object_id
        r = requests.put(
            url,
            data=payload,
            headers={
                'content-type': 'text/plain',
                'v-hash-fun': '0',
                'v-hash': hashlib.md5(payload).hexdigest(),
                'v-compress': 'lz4',
            }
        )
        assert 204 == r.status_code

        r = requests.get(url)
        assert 200 == r.status_code
        assert payload == r.text

    def test_etag(self):
        payload = "test"
        object_id = str(uuid.uuid4())
        url = self.endpoint + "/block/" + object_id
        r = requests.put(
            url,
            data=payload,
            headers={
                'content-type': 'text/plain',
                'v-hash-fun': '0',
                'v-hash': hashlib.md5(payload).hexdigest(),
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

    def test_compressed(self):
        payload = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Ut blandit rhoncus magna eget sollicitudin. Ut sem nisi, molestie quis neque ut, pellentesque sollicitudin velit. Quisque consequat risus erat, vitae volutpat est faucibus posuere. Nunc ultricies turpis non ipsum consectetur tincidunt sit amet sed urna. Donec fermentum ut elit ut auctor. Morbi et semper nulla, quis rhoncus massa. Curabitur consequat lorem sapien, vehicula tincidunt metus viverra nec. Nam consequat orci justo, ac malesuada diam scelerisque sed. Phasellus ut ligula purus. Ut elementum odio eget nunc euismod, eu pharetra eros efficitur. Donec faucibus massa eget mi pellentesque, bibendum sollicitudin elit efficitur. Nunc ante lacus, viverra vitae congue aliquet, molestie ut risus."
        payload += "Vivamus viverra molestie nulla, vel maximus odio vestibulum vitae. Quisque tempus blandit mi, a fringilla leo. In quis nulla quis quam fringilla tincidunt. Nam sit amet elit eget diam molestie mattis in sed ipsum. Sed tincidunt, odio at feugiat placerat, purus nisi tincidunt dolor, vitae ornare lorem nisl eget enim. Praesent neque dolor, tincidunt eget justo et, ullamcorper consectetur ligula. Ut vulputate arcu erat, in sagittis ligula cursus ac. Etiam vel cursus leo, vehicula tempus tortor. Donec ipsum risus, interdum id iaculis eget, condimentum a urna."
        payload += "Maecenas rhoncus euismod leo a dignissim. Nulla eu pellentesque orci, sed viverra odio. Mauris vulputate sapien ut consectetur consectetur. Vestibulum luctus lorem vitae ligula ornare, vitae congue nibh tempus. Suspendisse et aliquet felis. Proin eu sollicitudin purus. Aliquam erat volutpat. Donec porta massa a ex pretium lobortis. Ut cursus ac velit ut ultricies. Pellentesque porttitor tempor venenatis."
        payload += "Mauris vitae pellentesque odio, in varius orci. Fusce purus dui, tristique sed cursus id, consectetur et nisl. Ut finibus rhoncus mauris, at varius metus pretium quis. Interdum et malesuada fames ac ante ipsum primis in faucibus. Curabitur convallis ac nulla eget tempor. Suspendisse hendrerit nisi imperdiet mauris pellentesque ullamcorper. Nullam molestie rutrum mollis. Nunc pellentesque enim elit, sit amet aliquam ipsum venenatis quis. Proin pharetra arcu et bibendum vulputate. Fusce ac luctus sapien."
        payload += "Sed mattis tellus leo. Donec at vestibulum erat. Ut a lobortis nisi. Nunc volutpat, velit a iaculis dignissim, nulla tellus porttitor justo, et lobortis orci lorem a urna. Donec sit amet vulputate neque, nec aliquam lorem. Curabitur non nisi enim. Vestibulum lectus nulla, suscipit quis pretium ac, feugiat vulputate ante. Morbi rutrum vestibulum mi. Fusce semper rutrum mauris. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Nam ut erat ac nibh lacinia gravida. Donec convallis, massa vel facilisis vehicula, elit tortor lacinia libero, sit amet varius nibh enim a lorem. Suspendisse tincidunt elementum justo."
        payload += "Aliquam scelerisque metus ante. Nulla tempus quam diam, in consequat ex semper ut. Integer viverra urna odio, quis mattis metus elementum ac. Nullam id leo non dui fermentum pulvinar vel eu massa. Nunc porta tempor turpis vel ultrices. In ac arcu eu dui gravida euismod. Aenean pellentesque maximus magna, eget facilisis nisi vestibulum eget. Praesent laoreet velit eget rhoncus ullamcorper. Nulla facilisi. Morbi aliquet quam ut egestas convallis. Ut vel lorem id neque ultrices tincidunt in."
        object_id = str(uuid.uuid4())
        url = self.endpoint + "/block/" + object_id
        r = requests.put(
            url,
            data=payload,
            headers={
                'content-type': 'text/plain',
                'v-hash-fun': '0',
                'v-hash': hashlib.md5(payload).hexdigest(),
                'v-compress': 'lz4',
            }
        )
        assert 204 == r.status_code

        r = requests.get(
            url,
            headers={
                'accept-encoding': "deflate, lz4"
            }
        )
        assert 200 == r.status_code
        assert len(payload) > len(r.text)
        encoding = r.headers["content-encoding"]
        assert encoding == "lz4"