"""Порядок утренних бонусных задач.

Этот тест смотрит на форму кода, а не на поведение, и это осознанно. Ошибка,
которую он ловит, живёт ровно в порядке двух строк: начисление подарка тут же
берёт баланс для поздравления, а сгорание просроченных меняет его через доли
секунды. Поставь сгорание вторым — и письмо унесёт цифру, прожившую
80 миллисекунд.

Так и случилось: 329 клиентов получили поздравления с завышенным балансом,
пока 2026-08-29 клиент 1268 не позвонила спросить, куда делись её 260 бонусов
(в письме стояло 560, на счету было 300).

Воспроизвести это без базы нельзя, а цена ошибки — письмо клиенту с неверной
суммой, которое он запомнит и предъявит. Поэтому сторожим порядок вызовов.
"""

import ast
import unittest
from pathlib import Path

BOT_PY = Path(__file__).resolve().parent.parent / "bot.py"


def call_order(function_name: str, watched: set[str]) -> list[str]:
    """Имена интересующих нас вызовов внутри функции, в порядке строк."""
    tree = ast.parse(BOT_PY.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name != function_name:
            continue
        found = []
        for inner in ast.walk(node):
            if not isinstance(inner, ast.Call):
                continue
            name = getattr(inner.func, "id", None) or getattr(inner.func, "attr", None)
            if name in watched:
                found.append((inner.lineno, name))
        return [name for _, name in sorted(found)]
    raise AssertionError(f"функция {function_name} не найдена в bot.py")


class BirthdayJobOrderTests(unittest.TestCase):
    def test_expire_runs_before_accrual(self):
        order = call_order("run_birthday_jobs",
                           {"_expire_old_bonuses", "_accrue_birthday_bonuses"})
        self.assertEqual(order[:2],
                         ["_expire_old_bonuses", "_accrue_birthday_bonuses"],
                         "сначала сжигаем просроченное, потом дарим и пишем клиенту")

    def test_both_jobs_are_still_called(self):
        """Правка порядка не должна тихо выкинуть одну из задач."""
        order = call_order("run_birthday_jobs",
                           {"_expire_old_bonuses", "_accrue_birthday_bonuses"})
        self.assertIn("_expire_old_bonuses", order)
        self.assertIn("_accrue_birthday_bonuses", order)


if __name__ == "__main__":
    unittest.main()
