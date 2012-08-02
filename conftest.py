from tau import Tau, MemoryBackend, CSVBackend, BinaryBackend, ServerBackend


def pytest_generate_tests(metafunc):
    if 'tau' in metafunc.funcargnames:
        client = Tau(ServerBackend())
        memory = Tau(MemoryBackend(1))
        csv = Tau(CSVBackend('./tmp/'))
        binary = Tau(BinaryBackend('./tmp/'))
        metafunc.parametrize("tau", [client, memory, csv, binary])
