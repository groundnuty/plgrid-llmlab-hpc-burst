#!/usr/bin/env python3
"""Enumerate the request fields vLLM v0.32.0 accepts on /v1/chat/completions and /v1/completions.

Reads the class bodies from the pinned sources with the ast module, so every row carries the file and
line that declares the field. No module is imported or executed.

    python3 -I enumerate_fields.py --vllm <vllm checkout> --openai <unpacked openai wheel> > fields.tsv

Output columns (tab-separated): endpoint, json_path, class, field, annotation, default, source.
`json_path` is where the field sits in a request body: `[]` marks a list element, and a role or a part
type in parentheses names the variant of a union (`messages[](user).name`). The class-to-path table
below is the only hand-written part; a class it names that the source lacks is an error.
"""
import argparse
import ast
import os
import sys

V = "vllm"  # the vLLM checkout, relative paths below
O = "openai"  # the unpacked openai wheel

CHAT_PROTO = (V, "vllm/entrypoints/openai/chat_completion/protocol.py")
COMPL_PROTO = (V, "vllm/entrypoints/openai/completion/protocol.py")
BASE_PROTO = (V, "vllm/entrypoints/generate/base/protocol.py")
SAMPLING = (V, "vllm/sampling_params.py")
CHAT_UTILS = (V, "vllm/renderers/chat_utils.py")


def oa(name):
    return (O, "openai/types/chat/" + name + ".py")


# (endpoint, json path prefix, source file, class). Order is the order of the output.
CLASSES = [
    # Top-level request models.
    ("chat", "", CHAT_PROTO, "ChatCompletionRequest"),
    ("completions", "", COMPL_PROTO, "CompletionRequest"),
    # Nested models vLLM defines, reachable from the request models' annotations.
    ("chat", "tools[]", CHAT_PROTO, "ChatCompletionToolsParam"),
    ("chat", "tools[].function", BASE_PROTO, "FunctionDefinition"),
    ("chat", "tool_choice", CHAT_PROTO, "ChatCompletionNamedToolChoiceParam"),
    ("chat", "tool_choice.function", CHAT_PROTO, "ChatCompletionNamedFunction"),
    ("both", "stream_options", BASE_PROTO, "StreamOptions"),
    ("both", "response_format", BASE_PROTO, "ResponseFormat"),
    ("both", "response_format.json_schema", BASE_PROTO, "JsonSchemaResponseFormat"),
    ("both", "response_format(structural_tag)", BASE_PROTO, "StructuralTagResponseFormat"),
    ("both", "response_format(structural_tag, legacy)", BASE_PROTO, "LegacyStructuralTagResponseFormat"),
    ("both", "response_format(structural_tag, legacy).structures[]", BASE_PROTO, "LegacyStructuralTag"),
    ("both", "structured_outputs", SAMPLING, "StructuredOutputsParams"),
    ("both", "repetition_detection", SAMPLING, "RepetitionDetectionParams"),
    # Messages: vLLM's union is OpenAI's message params | CustomChatCompletionMessageParam | a Harmony
    # message (renderers/chat_utils.py:393). The Harmony message is openai_harmony's, not enumerated.
    ("chat", "messages[](custom)", CHAT_UTILS, "CustomChatCompletionMessageParam"),
    ("chat", "messages[](developer)", oa("chat_completion_developer_message_param"), "ChatCompletionDeveloperMessageParam"),
    ("chat", "messages[](system)", oa("chat_completion_system_message_param"), "ChatCompletionSystemMessageParam"),
    ("chat", "messages[](user)", oa("chat_completion_user_message_param"), "ChatCompletionUserMessageParam"),
    ("chat", "messages[](assistant)", oa("chat_completion_assistant_message_param"), "ChatCompletionAssistantMessageParam"),
    ("chat", "messages[](assistant).audio", oa("chat_completion_assistant_message_param"), "Audio"),
    ("chat", "messages[](assistant).function_call", oa("chat_completion_assistant_message_param"), "FunctionCall"),
    ("chat", "messages[](assistant).tool_calls[](function)", oa("chat_completion_message_function_tool_call_param"), "ChatCompletionMessageFunctionToolCallParam"),
    ("chat", "messages[](assistant).tool_calls[](function).function", oa("chat_completion_message_function_tool_call_param"), "Function"),
    ("chat", "messages[](assistant).tool_calls[](custom)", oa("chat_completion_message_custom_tool_call_param"), "ChatCompletionMessageCustomToolCallParam"),
    ("chat", "messages[](assistant).tool_calls[](custom).custom", oa("chat_completion_message_custom_tool_call_param"), "Custom"),
    ("chat", "messages[](tool)", oa("chat_completion_tool_message_param"), "ChatCompletionToolMessageParam"),
    ("chat", "messages[](function)", oa("chat_completion_function_message_param"), "ChatCompletionFunctionMessageParam"),
    # Content parts: OpenAI's (text, image_url, input_audio, file, refusal) and vLLM's own.
    ("chat", "messages[].content[](text)", oa("chat_completion_content_part_text_param"), "ChatCompletionContentPartTextParam"),
    ("chat", "messages[].content[](image_url)", oa("chat_completion_content_part_image_param"), "ChatCompletionContentPartImageParam"),
    ("chat", "messages[].content[](image_url).image_url", oa("chat_completion_content_part_image_param"), "ImageURL"),
    ("chat", "messages[].content[](input_audio)", oa("chat_completion_content_part_input_audio_param"), "ChatCompletionContentPartInputAudioParam"),
    ("chat", "messages[].content[](input_audio).input_audio", oa("chat_completion_content_part_input_audio_param"), "InputAudio"),
    ("chat", "messages[].content[](file)", oa("chat_completion_content_part_param"), "File"),
    ("chat", "messages[].content[](file).file", oa("chat_completion_content_part_param"), "FileFile"),
    ("chat", "messages[].content[](refusal)", oa("chat_completion_content_part_refusal_param"), "ChatCompletionContentPartRefusalParam"),
    ("chat", "messages[].content[](audio_url)", CHAT_UTILS, "ChatCompletionContentPartAudioParam"),
    ("chat", "messages[].content[](audio_url).audio_url", CHAT_UTILS, "AudioURL"),
    ("chat", "messages[].content[](video_url)", CHAT_UTILS, "ChatCompletionContentPartVideoParam"),
    ("chat", "messages[].content[](video_url).video_url", CHAT_UTILS, "VideoURL"),
    ("chat", "messages[].content[](image_embeds)", CHAT_UTILS, "ChatCompletionContentPartImageEmbedsParam"),
    ("chat", "messages[].content[](audio_embeds)", CHAT_UTILS, "ChatCompletionContentPartAudioEmbedsParam"),
    ("chat", "messages[].content[](video_embeds)", CHAT_UTILS, "ChatCompletionContentPartVideoEmbedsParam"),
    ("chat", "messages[].content[](prompt_embeds)", CHAT_UTILS, "ChatCompletionContentPartPromptEmbedsParam"),
    ("chat", "messages[].content[](image_pil)", CHAT_UTILS, "CustomChatCompletionContentPILImageParam"),
    ("chat", "messages[].content[](simple image)", CHAT_UTILS, "CustomChatCompletionContentSimpleImageParam"),
    ("chat", "messages[].content[](simple audio)", CHAT_UTILS, "CustomChatCompletionContentSimpleAudioParam"),
    ("chat", "messages[].content[](simple video)", CHAT_UTILS, "CustomChatCompletionContentSimpleVideoParam"),
    ("chat", "messages[].content[](thinking)", CHAT_UTILS, "CustomThinkCompletionContentParam"),
    ("chat", "messages[].content[](tool_reference)", CHAT_UTILS, "CustomChatCompletionContentToolReferenceParam"),
]


def is_classvar(ann):
    node = ann.value if isinstance(ann, ast.Subscript) else ann
    return isinstance(node, (ast.Name, ast.Attribute)) and ast.unparse(node).split(".")[-1] == "ClassVar"


def fields_of(tree, cls, path):
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == cls:
            rows = []
            for stmt in node.body:
                if not isinstance(stmt, ast.AnnAssign) or not isinstance(stmt.target, ast.Name):
                    continue
                name = stmt.target.id
                if name.startswith("_") or is_classvar(stmt.annotation):
                    continue  # private dataclass fields (init=False) and class variables are not request fields
                default = "" if stmt.value is None else " ".join(ast.unparse(stmt.value).split())
                rows.append((name, " ".join(ast.unparse(stmt.annotation).split()), default, stmt.lineno))
            bases = [ast.unparse(b) for b in node.bases]
            return rows, bases
    sys.exit(f"{path}: class {cls} not found")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vllm", required=True)
    ap.add_argument("--openai", required=True)
    args = ap.parse_args()
    roots = {V: args.vllm, O: args.openai}
    labels = {V: "vllm v0.32.0", O: "openai 2.25.0"}
    trees = {}
    out = sys.stdout
    out.write("endpoint\tjson_path\tclass\tfield\tannotation\tdefault\tsource\n")
    for endpoint, prefix, (root, rel), cls in CLASSES:
        key = (root, rel)
        if key not in trees:
            with open(os.path.join(roots[root], rel), encoding="utf-8") as f:
                trees[key] = ast.parse(f.read(), filename=rel)
        rows, bases = fields_of(trees[key], cls, rel)
        # Every class here derives from OpenAIBaseModel, BaseModel or TypedDict, or is a dataclass; a
        # base outside that set could add inherited fields, so it is refused rather than missed.
        for b in bases:
            if b not in ("OpenAIBaseModel", "BaseModel", "TypedDict"):
                sys.exit(f"{rel}: {cls} has base {b}; inherited fields would be missed")
        for name, ann, default, line in rows:
            jp = f"{prefix}.{name}" if prefix else name
            out.write(f"{endpoint}\t{jp}\t{cls}\t{name}\t{ann}\t{default}\t{labels[root]} {rel}:{line}\n")


if __name__ == "__main__":
    main()
