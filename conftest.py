from tau import Tau, TauClient, MemoryBackend, CSVBackend, BinaryBackend


def pytest_generate_tests(metafunc):
    if 'tau' in metafunc.funcargnames:
        memory = Tau(MemoryBackend(1))
        csv = Tau(CSVBackend('./tmp/'))
        binary = Tau(BinaryBackend('./tmp/'))
        metafunc.parametrize("tau", [memory, csv, binary])
