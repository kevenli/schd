import unittest
from schd.util import ensure_bool

class EnsureBoolTest(unittest.TestCase):
    def test_ensure_bool(self):
        self.assertEqual(ensure_bool(True), True)     # True
        self.assertEqual(ensure_bool(False), False)    # False
        self.assertEqual(ensure_bool(1), True)        # True
        self.assertEqual(ensure_bool(0), False)        # False
        self.assertEqual(ensure_bool(3.14), True)     # True
        self.assertEqual(ensure_bool(0.0), False)      # False
        self.assertEqual(ensure_bool("true"), True)   # True
        self.assertEqual(ensure_bool("False"), False)  # False
        self.assertEqual(ensure_bool("YES"), True)    # True
        self.assertEqual(ensure_bool("no"), False)     # False
        with self.assertRaises(ValueError):
            self.assertEqual(ensure_bool("random")) # Raises ValueError
