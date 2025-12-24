from __future__ import annotations

import importlib
from typing import Any


def _import_module(path: str) -> Any:
    return importlib.import_module(path)


def import_marketdata():
    try:
        marketdata_pb2 = _import_module("tinkoff.invest.grpc.marketdata_pb2")
        marketdata_pb2_grpc = _import_module("tinkoff.invest.grpc.marketdata_pb2_grpc")
        return marketdata_pb2, marketdata_pb2_grpc, "tinkoff.invest.grpc"
    except ModuleNotFoundError:
        marketdata_pb2 = _import_module("t_tech.invest.grpc.marketdata_pb2")
        marketdata_pb2_grpc = _import_module("t_tech.invest.grpc.marketdata_pb2_grpc")
        return marketdata_pb2, marketdata_pb2_grpc, "t_tech.invest.grpc"


def import_instruments():
    try:
        instruments_pb2 = _import_module("tinkoff.invest.grpc.instruments_pb2")
        instruments_pb2_grpc = _import_module("tinkoff.invest.grpc.instruments_pb2_grpc")
        return instruments_pb2, instruments_pb2_grpc, "tinkoff.invest.grpc"
    except ModuleNotFoundError:
        instruments_pb2 = _import_module("t_tech.invest.grpc.instruments_pb2")
        instruments_pb2_grpc = _import_module("t_tech.invest.grpc.instruments_pb2_grpc")
        return instruments_pb2, instruments_pb2_grpc, "t_tech.invest.grpc"
