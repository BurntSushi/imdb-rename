FROM scratch

# Copie du binaire compilé en statique (musl)
COPY --chmod=755 imdb-rename /imdb-rename

# /data  → répertoire des fichiers IMDb (téléchargements + index)
# /media → répertoire contenant les fichiers média à renommer
VOLUME ["/data", "/media"]

# Le répertoire des données IMDb est configuré via la variable d'environnement
ENV IMDB_RENAME_DATA_DIR=/data

# Format de renommage appliqué par le binaire :
#   Films     → "Titre (Année).ext"
#   Episodes  → "S01E01 - Titre.ext"
#
# Utilisation typique :
#   docker run \
#     -v /chemin/local/imdb-data:/data \
#     -v /chemin/local/films:/media \
#     ghcr.io/<owner>/imdb-rename \
#     --follow /media
#
# Pour forcer le re-téléchargement des données IMDb :
#   docker run -v /chemin/local/imdb-data:/data ghcr.io/<owner>/imdb-rename --update-data

ENTRYPOINT ["/imdb-rename"]
CMD ["--follow", "/media"]

