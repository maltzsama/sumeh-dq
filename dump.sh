#!/bin/bash

OUTPUT="sumeh-dump.md"

# Limpar arquivo anterior
>"$OUTPUT"

# Cabeçalho do Markdown
cat >"$OUTPUT" <<'HEADER'
# Sumeh DQ - Codebase Dump

> Dump gerado automaticamente do repositório sumeh-dq

## Índice

HEADER

# Gerar índice
git ls-files |
  grep -v -E '\.(md|MD|png|jpg|jpeg|gif|svg|ico|log|iml|properties)$' |
  grep -v "CHANGELOG.md" |
  grep -v "LICENSE" |
  grep -v -E '(target|project/target|tmp)/' |
  while read -r file; do
    echo "- [$file](#${file//[\/.]/-})" >>"$OUTPUT"
  done

echo "" >>"$OUTPUT"
echo "---" >>"$OUTPUT"
echo "" >>"$OUTPUT"

# Função para remover docstrings Scala
remove_docstrings() {
  sed -E \
    -e '/^[[:space:]]*\/\*\*/,/\*\//d' \
    -e '/^[[:space:]]*\/\/\/ /d' \
    -e '/^[[:space:]]*\/\/\//d' \
    -e '/^[[:space:]]*\/\*\*/,/\*\//d'
}

# Adicionar cada arquivo
git ls-files |
  grep -v -E '\.(md|MD|png|jpg|jpeg|gif|svg|ico|log|iml|properties)$' |
  grep -v "CHANGELOG.md" |
  grep -v "LICENSE" |
  grep -v -E '(target|project/target|tmp)/' |
  while read -r file; do
    echo "## Arquivo: \`$file\`" >>"$OUTPUT"
    echo "" >>"$OUTPUT"
    echo '```scala' >>"$OUTPUT"

    # Remover docstrings e adicionar conteúdo
    if [[ "$file" == *.scala ]]; then
      cat "$file" | remove_docstrings >>"$OUTPUT"
    else
      cat "$file" >>"$OUTPUT"
    fi

    echo '```' >>"$OUTPUT"
    echo "" >>"$OUTPUT"
    echo "---" >>"$OUTPUT"
    echo "" >>"$OUTPUT"
  done

echo "✅ Dump gerado em: $OUTPUT"
