import threading
import queue
import collections
import time

class AlwaysOnPipeline:
    def __init__(self):
        """
        Khởi tạo hệ thống Always-on với giới hạn tài nguyên khắt khe.
        """
        # 1. Ring Buffer quản lý bộ nhớ: Lưu tối đa 3 giây âm thanh (ví dụ: 30 chunk x 0.1s)
        # Khi đầy, phần tử cũ nhất sẽ tự động bị đẩy ra -> Loại bỏ hoàn toàn Memory Leak.
        self.ring_buffer = collections.deque(maxlen=30)
        
        # 2. Thread-safe Queue giới hạn maxsize để xử lý Backpressure
        self.audio_queue = queue.Queue(maxsize=50)
        
        # Tải mô hình
        self.vad = load_silero_vad()
        self.asr_model = load_sense_voice()
        self.tts_model = load_tts()
        
        self.is_running = True

    def producer_audio_vad_thread(self):
        """
        THREAD 1: Thu âm và VAD (Background Listening). 
        Ưu tiên cực thấp để nhường CPU cho hệ thống xe.
        """
        while self.is_running:
            # Thu âm liên tục từ Micro
            raw_chunk = get_microphone_chunk() 
            self.ring_buffer.append(raw_chunk) 
            
            # Chạy VAD siêu nhẹ
            is_speech = self.vad.process(raw_chunk)
            
            if is_speech:
                try:
                    # Đẩy dữ liệu vào Queue một cách an toàn (Thread-safe)
                    # Timeout 0.05s để tránh block thread thu âm khi queue bị đầy
                    self.audio_queue.put(raw_chunk, timeout=0.05)
                except queue.Full:
                    # CƠ CHẾ DROP FRAME (BACKPRESSURE)
                    print("Queue đầy! Drop frame để tránh treo RAM và nhường CPU.")

    def consumer_asr_thread(self):
        """
        THREAD 2: Inference ASR & TTS (Active Inference).
        Chỉ thức dậy khi có dữ liệu trong Queue.
        """
        while self.is_running:
            try:
                # Lấy dữ liệu ra xử lý. Block (ngủ) tại đây tối đa 1s nếu queue trống.
                # Khi ngủ, thread này tiêu thụ 0% CPU.
                speech_chunk = self.audio_queue.get(timeout=1.0)
                
                # Chạy ASR
                text = self.asr_model.transcribe(speech_chunk)
                if text:
                    self.process_and_speak(text)
                
                self.audio_queue.task_done()
            except queue.Empty:
                # VAD không trigger, không có ai nói -> Tiếp tục ngủ
                continue

    def process_and_speak(self, text):
        # Áp dụng quy tắc Regex/Text-normalization cho hệ thống Code-switching
        normalized_text = apply_regex_rules(text)
        
        # Điều chỉnh trực tiếp tham số length_scale để thay đổi Prosody mà không tốn thêm CPU
        audio = self.tts_model.synthesize(normalized_text, length_scale=0.8) # Đọc nhanh hơn cho cảnh báo
        play_audio(audio)

    def start_system(self):
        # Khởi tạo kiến trúc đa luồng
        t1 = threading.Thread(target=self.producer_audio_vad_thread, daemon=True)
        t2 = threading.Thread(target=self.consumer_asr_thread, daemon=True)
        t1.start()
        t2.start()