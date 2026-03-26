import py_compile
for f in [r"tc_principal\pages\home_tc.py", r"tc_principal\shared.py"]:
    try:
        py_compile.compile(f, doraise=True)
        print(f"OK: {f}")
    except py_compile.PyCompileError as e:
        print(f"FAIL: {e}")
