"""Broker-backed integration suite.

A package so that the test modules can import the shared :class:`BrokerScratch`
from ``conftest`` by a relative import: a run collecting ``tests`` and
``integration`` together otherwise has two top-level modules named ``conftest``.
"""
