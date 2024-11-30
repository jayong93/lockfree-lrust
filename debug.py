import lldb

pointer_ty: lldb.SBType | None = None
desc_ptr_ty: lldb.SBType | None = None


def init_head(head_expr: str):
    global pointer_ty, desc_ptr_ty
    pointer_ty = (
        lldb.frame.GetValueForVariablePath(head_expr).type
    )
    desc_ptr_ty = (
        pointer_ty.GetPointeeType().template_args[0].members[-1].type.template_args[0].GetPointerType()
    )


def set_ptr_type(ty: lldb.SBType):
    global pointer_ty
    pointer_ty = ty


def run_expr(expr: str):
    value = lldb.value(lldb.frame.EvaluateExpression(expr))
    print(value)
    return value


def value_as_node(value: lldb.SBValue | lldb.value, output=False):
    if isinstance(value, lldb.value):
        value = value.sbvalue
    ptr, marked = deref_node(value)
    print(ptr)
    if output:
        print(ptr.deref)
        print(f"marked: {marked}")
    return lldb.value(ptr)


def expr_as_node(expr: str, output=False):
    ptr, marked = deref_node(lldb.frame.GetValueForVariablePath(expr))
    print(ptr)
    if output:
        print(ptr.deref)
        print(f"marked: {marked}")
    return lldb.value(ptr)


def deref_node(val: lldb.SBValue) -> tuple[lldb.SBValue, bool]:
    # `val` is `Linked<Node<..>>*`
    addr: int = val.data.uint64[0]
    if addr % 2 == 1:
        return (
            val.CreateValueFromData(
                "removed",
                lldb.SBData.CreateDataFromInt(addr - 1, size=8),
                type=pointer_ty,
            ),
            True,
        )
    return (val.Cast(pointer_ty), False)


def follow_from_tail(tail: lldb.value):
    cur_node = tail
    visited = set([int(cur_node)])
    while int(cur_node.value.prev[0].p.value) != 0:
        cur_node = value_as_node(cur_node.value.prev[0].p.value)
        if int(cur_node) in visited:
            print(f"{cur_node} was visited")
            break
        visited.add(int(cur_node))
    return cur_node
