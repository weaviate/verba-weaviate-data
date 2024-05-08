import os
import re
from datetime import datetime
import json

from goldenverba.components.document import Document

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

from wasabi import msg  # type: ignore[import]
from dotenv import load_dotenv

load_dotenv()


def retrieve_documentation():
    """Downloads the Weaviate documentation, preprocesses, and chunks it. Returns a list of full documents and a list of chunks
    @returns tuple[list[Document], list[Document]] - A tuple of list of documents and list of chunks
    """
    download_from_github(
        "weaviate",
        "weaviate-io",
        "developers/",
        os.environ.get("GITHUB_TOKEN", ""),
        "Documentation",
    )

def retrieve_code():
    download_from_github(
        "weaviate",
        "weaviate-io",
        "_includes/code",
        os.environ.get("GITHUB_TOKEN", ""),
        "Code",
    )


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


def retrieve_transcripts(
    api_key: str,
    channel_id: str,
    doc_type: str = "Video",
):
    """Downloads video transcript from YouTube
    @parameter api_key : str - YouTube API key
    @parameter channel_id : str - YouTube channel ID
    @parameter doc_type : str - Document type (code, blogpost, podcast)
    @returns list[Doc] - A list of spaCy documents
    """
    print(f"Starting downloading {doc_type} from channel ID {channel_id}")

    video_ids = get_all_video_ids(api_key, channel_id)
    fetch_transcripts(video_ids)


def download_from_github(
    owner: str,
    repo: str,
    folder_path: str,
    token: str = None,
    doc_type: str = "Documentation",
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
    docs = []
    for document_name in document_names:
        try:
            fetched_text, link, path = download_file(owner, repo, document_name, token)
        except Exception as e:
            msg.fail(str(e))

        if filtering(path, doc_type):
            text = cleaning(fetched_text, doc_type)
            if len(text) > 500:
                doc = Document(
                    text=cleaning(fetched_text, doc_type),
                    type=doc_type,
                    name=process_filename(str(path), doc_type),
                    path=path,
                    link=process_url(str(path), doc_type, fetched_text),
                    timestamp=str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    reader="JSON",
                )

                with open(f"data/{doc_type}/{doc.name}.json", "w") as writer:
                    json_obj = Document.to_json(doc)
                    json.dump(json_obj, writer)
                msg.info(f"Loaded and saved {doc.name}")
                docs.append(doc)

    msg.good(f"All {len(docs)} files successfully loaded")

    return docs


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
    if document_type == "Documentation" or document_type == "Blog" or document_type == "Code" :
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
    #retrieve_documentation()
    #retrieve_blogs()
    retrieve_code()
    #api, channel = load_configuration()
    #retrieve_transcripts(api, channel)
