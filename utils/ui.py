from aiogram.types import Message
from aiogram.fsm.context import FSMContext


def admin_root_kb():
    from bot import admin_root_kb as _kb  # type: ignore
    return _kb()


async def show_admin_menu(msg: Message, state: FSMContext, text: str = "Меню администратора:"):
    from bot import AdminMenuFSM  # type: ignore
    await state.set_state(AdminMenuFSM.root)
    await msg.answer(text, reply_markup=admin_root_kb())
