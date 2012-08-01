from tau import Tau, TauClient, MemoryBackend, CSVBackend


def pytest_generate_tests(metafunc):
    if 'tau' in metafunc.funcargnames:
        memory = Tau(MemoryBackend(1))
        csv = Tau(CSVBackend('./tmp/'))
        metafunc.parametrize("tau", [memory, csv])
