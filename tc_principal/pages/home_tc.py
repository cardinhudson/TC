import streamlit as st


def render() -> None:
    st.title("🏭 TC (Planta Principal)")
    st.info(
        "Página inicial do módulo TC (Planta Principal). "
        "Este módulo será incorporado aqui com a mesma disciplina de regras (CPU/Flex/ordem K/M→moeda→cálculos)."
    )

    st.caption("📚 Documentação do projeto: menu 'Documentação' no portal.")


if __name__ == "__main__":
    render()
