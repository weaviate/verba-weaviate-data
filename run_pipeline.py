import os
import re
from datetime import datetime
import json
import difflib

from goldenverba.components.document import Document
from goldenverba import verba_manager
from goldenverba.server.types import (
    Credentials,
    FileConfig,
    FileStatus,
    RAGComponentClass,
)

from weaviate.client import WeaviateAsyncClient

from fetch_github import (
    fetch_docs,
    download_file,
    is_link_working,
)

from transcript import (
    load_configuration,
    fetch_transcripts,
    get_all_video_ids,
    fetch_youtube_transcripts,
)

from retrieve_html_to_text import (
    get_href_from_homepage,
    get_markdown_from_url,
    recursive_get_hrefs,
)

from wasabi import msg  # type: ignore[import]
from dotenv import load_dotenv

load_dotenv()


def retrieve_code():
    download_from_github(
        "weaviate",
        "weaviate-io",
        "_includes/code",
        os.environ.get("GITHUB_TOKEN", ""),
        "Code",
    )


async def scrape_documentation(
    client: WeaviateAsyncClient,
    manager: verba_manager.VerbaManager,
    rag_config: dict[str, RAGComponentClass],
    verbose: bool = False,
):
    msg.divider(f"Starting scraping weaviate.io")

    try:
        doc_counter = 0
        max_docs = 100000

        links = set()

        msg.divider(f"Starting retrieval of {max_docs} documents")
        async for link in recursive_get_hrefs():
            try:
                if link in links:
                    continue
                links.add(link)
                if doc_counter >= max_docs:
                    break
                markdown = await get_markdown_from_url(link)

                doc_name = (
                    link.replace("https://weaviate.io/", "")
                    .replace("developers/weaviate/", "")
                    .replace("/", "_")
                    .replace("-", "_")
                    .replace("#", "_")
                    .replace("developers_weaviate_", "")
                )

                if len(markdown) < 1500 or "docusaurus_skipToContent" in doc_name:
                    continue

                file_config = FileConfig(
                    fileID=doc_name,
                    filename=doc_name,
                    isURL=False,
                    overwrite=False,
                    extension="",
                    source=link,
                    content=markdown,
                    labels=["Documentation"],
                    rag_config=rag_config,
                    file_size=len(markdown),
                    status=FileStatus.STARTING,
                    metadata="",
                    status_report={},
                )

                doc_counter += 1
                msg.info(f"Importing {file_config.filename} | {doc_counter}")
                await manager.import_document(client, file_config)
            except Exception as e:
                msg.fail(f"Failed to import {file_config.filename}: {e}")
                continue

        msg.good(f"All {doc_counter} files successfully loaded")
    except Exception as e:
        msg.fail(f"Failed to load documentation: {e}")


def find_common_substring(texts, verbose: bool = False):
    """Find common substrings at the beginning and end in pairs of texts."""
    if not texts:
        return []

    def common_prefix(s1, s2):
        """Find common substring at the beginning of s1 and s2."""
        min_len = min(len(s1), len(s2))
        for i in range(min_len):
            if s1[i] != s2[i]:
                return s1[:i]
        return s1[:min_len]

    def common_suffix(s1, s2):
        """Find common substring at the end of s1 and s2."""
        min_len = min(len(s1), len(s2))
        for i in range(1, min_len + 1):
            if s1[-i] != s2[-i]:
                return s1[-i + 1 :]
        return s1[-min_len:]

    common_substrings = []

    for i in range(len(texts)):
        for j in range(i + 1, len(texts)):
            common_prefix_str = common_prefix(texts[i], texts[j])
            common_suffix_str = common_suffix(texts[i], texts[j])
            if common_prefix_str or common_suffix_str:
                common_substrings.append((common_prefix_str, common_suffix_str))

    if verbose:
        with open("common_substring_debug.txt", "w") as debug_file:
            debug_file.write("Common Substrings:\n")
            for prefix, suffix in common_substrings:
                debug_file.write(f"Prefix: {prefix}, Suffix: {suffix}\n")
            debug_file.write("\nTexts:\n")
            for text in texts:
                debug_file.write(text + "\n")
                debug_file.write("-" * 40 + "\n")  # Divider

    return common_substrings


def remove_common_substrings(text, common_substrings):
    """Remove common substrings from a text."""
    for prefix, suffix in common_substrings:
        if text.startswith(prefix):
            text = text[len(prefix) :]
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def retrieve_blogs():
    """Downloads the Weaviate documentation, preprocesses, and chunks it. Returns a list of full documents and a list of chunks
    @returns tuple[list[Document], list[Document]] - A tuple of list of documents and list of chunks
    """
    download_from_github(
        "weaviate",
        "weaviate-io",
        "blog/",
        os.environ.get("GITHUB_TOKEN", ""),
        "Blog",
    )


async def retrieve_transcripts(
    api_key: str,
    channel_id: str,
    doc_type: str = "Video",
    client: WeaviateAsyncClient = None,
    manager: verba_manager.VerbaManager = None,
    rag_config: dict[str, RAGComponentClass] = None,
):
    """Downloads video transcript from YouTube
    @parameter api_key : str - YouTube API key
    @parameter channel_id : str - YouTube channel ID
    @parameter doc_type : str - Document type (code, blogpost, podcast)
    @returns list[Doc] - A list of spaCy documents
    """
    print(f"Starting downloading {doc_type} from channel ID {channel_id}")

    video_ids = get_all_video_ids(api_key, channel_id)
    for whole_text, title, link in fetch_transcripts(video_ids):
        if whole_text is not None:
            file_config = FileConfig(
                fileID=title,
                filename=title,
                isURL=False,
                overwrite=False,
                extension="",
                source=link,
                content=whole_text,
                labels=[doc_type],
                rag_config=rag_config,
                file_size=len(whole_text),
                status=FileStatus.STARTING,
                metadata="",
                status_report={},
            )
            msg.info(f"Importing {file_config.filename}")
            await manager.import_document(client, file_config)


async def download_from_github(
    owner: str,
    repo: str,
    folder_path: str,
    token: str = None,
    doc_type: str = "Documentation",
    client: WeaviateAsyncClient = None,
    manager: verba_manager.VerbaManager = None,
    rag_config: dict[str, RAGComponentClass] = None,
):
    """Downloads .mdx/.md files from Github
    @parameter owner : str - Repo owner
    @parameter repo : str - Repo name
    @parameter folder_path : str - Directory in repo to fetch from
    @parameter token : str - Github token
    @parameter doc_type : str - Document type (code, blogpost, podcast)
    @returns list[Doc] - A list of spaCy documents
    """
    msg.divider(f"Starting downloading {doc_type} from {owner}/{repo}/{folder_path}")
    document_names = fetch_docs(owner, repo, folder_path, token)
    msg.info(f"Found {len(document_names)} documents")

    doc_counter = 0
    max_docs = 10000

    for document_name in document_names:
        try:
            if doc_counter >= max_docs:
                break
            fetched_text, link, path = await download_file(
                owner, repo, document_name, token
            )
        except Exception as e:
            msg.fail(str(e))

        if filtering(path, doc_type):
            text = cleaning(fetched_text, doc_type)
            if len(text) > 1500:

                file_config = FileConfig(
                    fileID=process_filename(str(path), doc_type),
                    filename=process_filename(str(path), doc_type),
                    isURL=False,
                    overwrite=False,
                    extension="",
                    source=process_url(str(path), doc_type, fetched_text),
                    content=cleaning(fetched_text, doc_type),
                    labels=[doc_type],
                    rag_config=rag_config,
                    file_size=len(cleaning(fetched_text, doc_type)),
                    status=FileStatus.STARTING,
                    metadata="",
                    status_report={},
                )

                doc_counter += 1
                msg.info(f"Importing {file_config.filename} | {doc_counter}")
                await manager.import_document(client, file_config)

    msg.good(f"All {len(doc_counter)} files successfully loaded")


# Data Filtering


def filtering(document_path: str, document_type: str) -> bool:
    """Filters documents based on their path and document type
    @parameter document_path : str - Document Path
    @parameter document_type : str - Document Type
    @returns bool - Flag whether they should be included or not
    """
    if document_type == "Documentation" or document_type == "Blog":
        return document_filtering(document_path)
    else:
        return True


def document_filtering(document_path: str) -> bool:
    """Filters documents based on their path tailored towards Weaviate documentation .md files
    @parameter document_path : str - Document Path
    @returns bool - Flag whether they should be included or not
    """
    # Split the document path into its components
    components = document_path.split("/")
    # Check if any component starts with '_'
    for component in components:
        if component.startswith("_"):
            msg.warn(f"Skipping {document_path}")
            return False
    return True


# Cleaning


def cleaning(document_str: str, document_type: str) -> str:
    """Preprocess and clean documents from mdx markings
    @parameter document_str : str - Document text
    @parameter document_type : str - Document Type
    @returns str - The preprocessed and cleaned document text
    """

    if document_type == "Documentation" or document_type == "Blog":
        return document_cleaning(document_str)

    return document_str


def document_cleaning(document_str: str) -> str:
    """Preprocess and clean documents from mdx markings tailored towards Weaviate documentation .md files
    @parameter document_str : str - Document text
    @returns str - The preprocessed and cleaned document text
    """
    # Step 0: Remove everything between the starting '---' pair
    text = re.sub(r"^---.*?---\n?", "", document_str, flags=re.DOTALL)

    # Step 1: Remove everything above <!-- truncate -->
    text = re.sub(r"(?s)^.*?<!-- truncate -->\n?", "", text)

    # Step 2: Remove import statements
    text = re.sub(r"import\s+.*?from\s+['\"].*?['\"];\s*", "", text, flags=re.MULTILINE)

    # Remove all HTML-like tags
    text = re.sub(r"<[^>]+>", "", text)

    # Step 4: Remove tags with three double dots and their corresponding closing tags
    text = re.sub(r":::.*?\n", "", text)
    text = re.sub(r":::\n?", "", text)

    # Step 5: Replace markdown image and link references with their text
    # text = re.sub(r"!\[(.*?)\]\(.*?\)", r"\1", text)  # Image links
    # text = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", text)  # Normal links

    return text


# Filename Processing


def process_filename(file_path: str, document_type: str) -> str:
    """Preprocess filename based on file path and document type
    @parameter document_str : str - Document text
    @parameter document_type : str - Document Type
    @returns str - The preprocessed filename
    """
    if (
        document_type == "Documentation"
        or document_type == "Blog"
        or document_type == "Code"
    ):
        return document_process_filename(file_path)
    else:
        return file_path


def document_process_filename(file_path: str) -> str:
    """Preprocess filename based on file path and document type tailored towards Weaviate Documentation
    @parameter file_path : str - Document path
    @returns str - The preprocessed filename
    """
    # Split the path into its components
    parts = file_path.split(os.sep)

    # Check if there are at least two parts to extract
    if len(parts) < 2:
        return file_path

    # Remove the file extension from the last part
    filename_without_extension = os.path.splitext(parts[-1])[0]

    # If the filename is "index", use the third last and second last parts
    if filename_without_extension == "index":
        base_name = parts[-2]
        prefix = parts[-3] if len(parts) >= 3 else ""
    else:
        base_name = filename_without_extension
        prefix = parts[-2]

    # Clean the prefix and base name
    prefix = re.sub(r"^\d+_", "", prefix)
    prefix = re.sub(r"\d{4}-\d{2}-\d{2}-", "", prefix)
    base_name = re.sub(r"^\d+_", "", base_name)
    base_name = re.sub(r"\d{4}-\d{2}-\d{2}-", "", base_name)

    # Capitalize the first letter of both prefix and base name
    prefix = prefix.capitalize()
    base_name = base_name.capitalize()

    return " ".join([prefix, base_name]) if prefix else base_name


# URL Processing


def process_url(file_path: str, document_type: str, document_text: str = "") -> str:
    """Preprocess filename to a URL based on document type, also checks whether the link is valid
    @parameter document_str : str - Document text
    @parameter document_type : str - Document Type
    @returns str - A valid url linking to the document
    """
    processed_url = ""

    if document_type == "Documentation":
        processed_url = document_process_url(file_path)
    elif document_type == "Blog":
        processed_url = blog_process_url(document_text)
    else:
        base_url = "https://weaviate.io/"

        # Remove the file extension
        without_extension = os.path.splitext(file_path)[0]

        # Concatenate the base_url with the modified path
        full_url = os.path.join(base_url, without_extension)

        processed_url = full_url

    if not is_link_working(processed_url):
        msg.warn(f"{processed_url} not working!")

    return processed_url


def document_process_url(file_path: str) -> str:
    """Preprocess filename to a URL based on document type tailored towards Weaviate Documentation .md files
    @parameter document_str : str - Document text
    @returns str - A valid url linking to the document
    """
    base_url = "https://weaviate.io/"

    # Remove the file extension
    without_extension = os.path.splitext(file_path)[0]

    # Break down the path into individual components
    components = without_extension.split(os.sep)

    # Process each component
    for i, component in enumerate(components):
        # Remove leading numbers and underscores
        while component and (component[0].isdigit() or component[0] == "_"):
            component = component[1:]
        components[i] = component

    # Join the modified components
    modified_path = os.sep.join(components)

    # Concatenate the base_url with the modified path
    full_url = os.path.join(base_url, modified_path)

    # Remove trailing "/index"
    if full_url.endswith("/index"):
        full_url = full_url[:-6]

    return full_url


def blog_process_url(document_text: str) -> str:
    """Preprocess filename to a URL based on document type tailored towards Weaviate Blog .md files
    @parameter document_str : str - Document text
    @returns str - A valid url linking to the document
    """
    base_url = "https://weaviate.io/blog/"

    def extract_slug(text: str) -> str:
        """Extract slug content from the provided text."""
        match = re.search(r"slug:\s*(\S+)", text)
        return match.group(1) if match else None

    slug = extract_slug(document_text)

    full_url = base_url + slug

    return full_url


if __name__ == "__main__":
    import asyncio

    async def main():
        try:
            manager = verba_manager.VerbaManager()
            credentials = Credentials(
                deployment="Weaviate",
                url=os.getenv("WEAVIATE_URL_VERBA"),
                key=os.getenv("WEAVIATE_API_KEY_VERBA"),
            )
            client: WeaviateAsyncClient = await manager.connect(credentials)

            msg.info("Deleting all documents")

            # await manager.weaviate_manager.delete_all_documents(client)

            rag_config = await manager.load_rag_config(client)

            if rag_config is not None:
                # await scrape_documentation(client, manager, rag_config)
                # await download_from_github(
                #     "weaviate",
                #     "weaviate-io",
                #     "blog/",
                #     os.environ.get("GITHUB_TOKEN", ""),
                #     "Blog",
                #     client,
                #     manager,
                #     rag_config,
                # )
                api, channel = load_configuration()
                await retrieve_transcripts(
                    api,
                    channel,
                    "Video",
                    client,
                    manager,
                    rag_config,
                )
            else:
                msg.fail("Failed to load RAG config")

            await client.close()
        except Exception as e:
            msg.fail(f"Failed to run pipeline: {e}")
            await client.close()

    asyncio.run(main())
