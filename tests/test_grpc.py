import pytest
import requests
import uuid
import hashlib
import os
import grpc
from client import block_api_pb2
from client import block_api_pb2_grpc


class TestGrpcApi:
    endpoint = "127.0.0.1:33085"
    payload = '''
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Ut blandit rhoncus magna eget sollicitudin. Ut sem nisi, molestie quis neque ut, pellentesque sollicitudin velit. Quisque consequat risus erat, vitae volutpat est faucibus posuere. Nunc ultricies turpis non ipsum consectetur tincidunt sit amet sed urna. Donec fermentum ut elit ut auctor. Morbi et semper nulla, quis rhoncus massa. Curabitur consequat lorem sapien, vehicula tincidunt metus viverra nec. Nam consequat orci justo, ac malesuada diam scelerisque sed. Phasellus ut ligula purus. Ut elementum odio eget nunc euismod, eu pharetra eros efficitur. Donec faucibus massa eget mi pellentesque, bibendum sollicitudin elit efficitur. Nunc ante lacus, viverra vitae congue aliquet, molestie ut risus.
        Vivamus viverra molestie nulla, vel maximus odio vestibulum vitae. Quisque tempus blandit mi, a fringilla leo. In quis nulla quis quam fringilla tincidunt. Nam sit amet elit eget diam molestie mattis in sed ipsum. Sed tincidunt, odio at feugiat placerat, purus nisi tincidunt dolor, vitae ornare lorem nisl eget enim. Praesent neque dolor, tincidunt eget justo et, ullamcorper consectetur ligula. Ut vulputate arcu erat, in sagittis ligula cursus ac. Etiam vel cursus leo, vehicula tempus tortor. Donec ipsum risus, interdum id iaculis eget, condimentum a urna.
        Maecenas rhoncus euismod leo a dignissim. Nulla eu pellentesque orci, sed viverra odio. Mauris vulputate sapien ut consectetur consectetur. Vestibulum luctus lorem vitae ligula ornare, vitae congue nibh tempus. Suspendisse et aliquet felis. Proin eu sollicitudin purus. Aliquam erat volutpat. Donec porta massa a ex pretium lobortis. Ut cursus ac velit ut ultricies. Pellentesque porttitor tempor venenatis.
        Mauris vitae pellentesque odio, in varius orci. Fusce purus dui, tristique sed cursus id, consectetur et nisl. Ut finibus rhoncus mauris, at varius metus pretium quis. Interdum et malesuada fames ac ante ipsum primis in faucibus. Curabitur convallis ac nulla eget tempor. Suspendisse hendrerit nisi imperdiet mauris pellentesque ullamcorper. Nullam molestie rutrum mollis. Nunc pellentesque enim elit, sit amet aliquam ipsum venenatis quis. Proin pharetra arcu et bibendum vulputate. Fusce ac luctus sapien.
        Sed mattis tellus leo. Donec at vestibulum erat. Ut a lobortis nisi. Nunc volutpat, velit a iaculis dignissim, nulla tellus porttitor justo, et lobortis orci lorem a urna. Donec sit amet vulputate neque, nec aliquam lorem. Curabitur non nisi enim. Vestibulum lectus nulla, suscipit quis pretium ac, feugiat vulputate ante. Morbi rutrum vestibulum mi. Fusce semper rutrum mauris. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Nam ut erat ac nibh lacinia gravida. Donec convallis, massa vel facilisis vehicula, elit tortor lacinia libero, sit amet varius nibh enim a lorem. Suspendisse tincidunt elementum justo.
        Aliquam scelerisque metus ante. Nulla tempus quam diam, in consequat ex semper ut. Integer viverra urna odio, quis mattis metus elementum ac. Nullam id leo non dui fermentum pulvinar vel eu massa. Nunc porta tempor turpis vel ultrices. In ac arcu eu dui gravida euismod. Aenean pellentesque maximus magna, eget facilisis nisi vestibulum eget. Praesent laoreet velit eget rhoncus ullamcorper. Nulla facilisi. Morbi aliquet quam ut egestas convallis. Ut vel lorem id neque ultrices tincidunt in.
    '''
    random_payload = "\x00" + os.urandom(4 * 1024 * 1024) + "\x00"
    client = None

    @classmethod
    def setup_class(cls):
        channel = grpc.insecure_channel(cls.endpoint)
        stub = block_api_pb2_grpc.BlockApiStub(channel)
        cls.client = stub

    def test_index(self):
        res = self.client.Idx(block_api_pb2.IdxRequest())
        assert "The little block engine that could!" == res.message

    def test_status(self):
        res = self.client.Status(block_api_pb2.StatusRequest())
        assert "normal" == res.node.status

    def test_insert(self):
        block_id = str(uuid.uuid4())
        object_id = str(uuid.uuid4())

        insert_request = block_api_pb2.InsertRequest(
            block_id=block_id,
            object_id=object_id,
            payload=self.payload,
            options=block_api_pb2.WriteOptions(
                content_type='plain/text',
                compress=False,
                hash=hashlib.md5(self.payload).hexdigest(),
                hash_fun=block_api_pb2.MD5
            )
        )

        res = self.client.Insert(insert_request)
        assert res.block_id == block_id
        assert res.meta.size == len(self.payload)

        try:
            self.client.Insert(insert_request)
            raise Exception("written not uniq")
        except:
            pass

    def test_upsert(self):
        block_id = str(uuid.uuid4())
        object_id = str(uuid.uuid4())

        insert_request = block_api_pb2.InsertRequest(
            block_id=block_id,
            object_id=object_id,
            payload=self.payload,
            options=block_api_pb2.WriteOptions(
                content_type='plain/text',
                compress=False,
                hash=hashlib.md5(self.payload).hexdigest(),
                hash_fun=block_api_pb2.MD5
            )
        )

        res = self.client.Insert(insert_request)
        assert res.block_id == block_id
        assert res.meta.size == len(self.payload)

        upsert_request = block_api_pb2.UpsertRequest(
            block_id=block_id,
            object_id=object_id,
            payload=self.payload,
            options=block_api_pb2.WriteOptions(
                content_type='plain/text',
                compress=False,
                hash=hashlib.md5(self.payload).hexdigest(),
                hash_fun=block_api_pb2.MD5
            )
        )

        res = self.client.Upsert(upsert_request)
        assert res.block_id == block_id
        assert res.meta.size == len(self.payload)
        # one more time
        res = self.client.Upsert(upsert_request)
        assert res.block_id == block_id
        assert res.meta.size == len(self.payload)

    def test_get(self):
        block_id = str(uuid.uuid4())
        object_id = str(uuid.uuid4())

        insert_request = block_api_pb2.InsertRequest(
            block_id=block_id,
            object_id=object_id,
            payload=self.payload,
            options=block_api_pb2.WriteOptions(
                content_type='plain/text',
                compress=False,
                hash=hashlib.md5(self.payload).hexdigest(),
                hash_fun=block_api_pb2.MD5
            )
        )
        res = self.client.Insert(insert_request)
        assert res.block_id == block_id
        assert res.meta.size == len(self.payload)

        get_request = block_api_pb2.GetRequest(
            block_id=block_id,
            crc="",
            allow_compressed=False,
        )
        res = self.client.Get(get_request)
        assert res.block_id == block_id
        assert res.meta.size == len(self.payload)
        assert res.payload == self.payload
        assert res.not_modified == False

        get_not_modified = block_api_pb2.GetRequest(
            block_id=block_id,
            crc=res.meta.crc,
            allow_compressed=False,
        )
        res = self.client.Get(get_not_modified)
        assert res.not_modified == True

        get_404 = block_api_pb2.GetRequest(
            block_id="!!!unknown!!!",
            crc="",
            allow_compressed=False,
        )
        try:
            res = self.client.Get(get_404)
            raise Exception("found")
        except:
            pass

    def test_append(self):
        block_id = str(uuid.uuid4())
        payload = "text1"
        insert_request = block_api_pb2.InsertRequest(
            block_id=block_id,
            payload=payload,
            options=block_api_pb2.WriteOptions(
                content_type='plain/text',
                compress=False,
            )
        )
        res = self.client.Insert(insert_request)
        assert res.block_id == block_id
        assert res.meta.size == len(payload)

        get_request = block_api_pb2.GetRequest(
            block_id=block_id,
        )
        res = self.client.Get(get_request)
        assert res.payload == "text1"

        append_request = block_api_pb2.AppendRequest(
            block_id = block_id,
            payload = "text2",
            options=block_api_pb2.WriteOptions(
                content_type='plain/text',
                compress=False,
            )
        )
        res = self.client.Append(append_request)
        assert res.block_id == block_id
        assert res.meta.size == len("text1text2")

        res = self.client.Get(get_request)
        assert res.payload == "text1text2"
        assert res.meta.size == len("text1text2")

    def test_delete(self):
        block_id = str(uuid.uuid4())
        payload = "text1"
        insert_request = block_api_pb2.InsertRequest(
            block_id=block_id,
            payload=payload,
            options=block_api_pb2.WriteOptions(
                content_type='plain/text',
                compress=False,
            )
        )
        res = self.client.Insert(insert_request)
        assert res.block_id == block_id

        get_request = block_api_pb2.GetRequest(
            block_id=block_id,
        )
        res = self.client.Get(get_request)
        assert res.payload == "text1"

        delete_request = block_api_pb2.DeleteRequest(
            block_id = block_id
        )
        res = self.client.Delete(delete_request)
        assert res.block_id == block_id

        try:
            res = self.client.Get(get_request)
            raise Exception("found")
        except:
            pass