import os
import json
from medical_agent import MedicalAgent
from datetime import datetime

class AcademicAgent(MedicalAgent):
    def __init__(self):
        super().__init__()
        print("AcademicAgent initialized with citation capabilities.")

    def format_citation(self, metadata, style="APA"):
        """
        Formats a citation based on common metadata.
        metadata: {title, author, year, source, url}
        """
        author = metadata.get("author", "Unknown Author")
        year = metadata.get("year", datetime.now().year)
        title = metadata.get("title", "No Title")
        source = metadata.get("source", "Medical Database")
        url = metadata.get("url", "")

        if style == "APA":
            # APA: Author, A. A. (Year). Title of work. Source. URL
            return f"{author}. ({year}). {title}. {source}. {url}"
        elif style == "MLA":
            # MLA: Author. "Title." Source, Year, URL.
            return f"{author}. \"{title}.\" {source}, {year}, {url}."
        return f"{title} ({source})"

    def extract_metadata_from_text(self, title, description):
        """Uses LLM to guess academic metadata if not explicitly in DB."""
        prompt = f"""
        Extract academic metadata from this clinical document title and snippet.
        Title: {title}
        Snippet: {description[:500]}
        
        Output ONLY a JSON object with: author, year, institution, city.
        If unknown, use null.
        """
        
        try:
            response = self.chat_client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            return json.loads(response.choices[0].message.content)
        except:
            return {"author": "Bộ Y tế", "year": "2024", "institution": "Cục Quản lý Khám chữa bệnh"}

    def generate_academic_response(self, user_query, style="APA", context_nodes=None):
        """Generates a response with proper academic citations.

        If context_nodes is provided, uses them directly (avoids redundant search).
        Otherwise falls back to atomic_search → hybrid_search.
        """
        if not context_nodes:
            context_nodes = self.atomic_search(user_query)
            if not context_nodes:
                context_nodes = self.hybrid_search(user_query)

        if not context_nodes:
            return (
                "Chưa tìm thấy đủ tri thức trong Pathway để trả lời có căn cứ cho câu hỏi này. "
                "Hiện hệ thống chưa lấy được đoạn protocol hoặc ontology evidence phù hợp, nên không nên kết luận. "
                "Đề nghị truy thêm RawChunk, ProtocolAssertion, hoặc testcase liên quan trước khi trả lời chắc chắn."
            )
        
        # Prepare context with citations
        formatted_context = []
        citations_list = []
        
        for i, node in enumerate(context_nodes):
            meta = self.extract_metadata_from_text(node['title'], node['description'])
            meta.update({
                "title": node['title'],
                "url": node.get('url', ''),
                "source": node.get('source', 'Medical Protocol')
            })

            citation = self.format_citation(meta, style=style)
            citations_list.append(citation)

            # Use expanded description (includes prev/next context) if available
            content = node.get('expanded_description') or node.get('description', '')
            node_context = f"SOURCE [{i+1}]:\n"
            node_context += f"Citation: {citation}\n"
            node_context += f"Content: {content}\n"
            formatted_context.append(node_context)

        context_str = "\n---\n".join(formatted_context)
        
        system_prompt = f"""Bạn là một chuyên gia nghiên cứu y khoa đang hoạt động bên trong Pathway.
Chỉ được kết luận những gì được hỗ trợ trực tiếp bởi các nguồn đã cung cấp dưới đây.
Nếu nguồn không đủ để khẳng định một ý nào đó, phải nói rõ là chưa đủ bằng chứng trong Pathway hiện tại.
Không được dùng kiến thức ngoài ngữ cảnh để biến một khoảng trống tri thức thành kết luận chắc chắn.
Sử dụng trích dẫn trong văn bản dưới dạng [1], [2] tương ứng với nguồn.
Nếu có khoảng trống, nêu ngắn gọn Pathway cần truy thêm tri thức gì.
Cuối câu trả lời, hãy tạo một mục 'Danh mục tài liệu tham khảo' định dạng theo chuẩn {style}.
Phong cách trả lời: Khách quan, học thuật, chính xác, có kiểm soát groundedness."""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Tài liệu nghiên cứu:\n{context_str}\n\nCâu hỏi nghiên cứu: {user_query}"}
        ]

        response = self.chat_client.chat.completions.create(
            model=self.model,
            messages=messages,
        )
        
        return response.choices[0].message.content

if __name__ == "__main__":
    agent = AcademicAgent()
    try:
        q = "Các bài thuốc YHCT điều trị mất ngủ thể tâm tỳ lưỡng hư?"
        print(f"QUESTION: {q}")
        print("GENERATE APA STYLE RESPONSE...")
        print(agent.generate_academic_response(q, style="APA"))
    finally:
        agent.close()
