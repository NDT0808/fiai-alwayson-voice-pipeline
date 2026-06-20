# Giải Pháp Hệ Thống Trợ Lý Ảo Always-On Cho Bảng Điều Khiển Xe Điện

Dự án này phác thảo kiến trúc đa luồng (**Producer–Consumer**) và cơ chế quản lý tài nguyên (**CPU/RAM**) cho hệ thống trợ lý ảo giám sát trên Raspberry Pi 5. Hệ thống yêu cầu microphone luôn mở (**Always-on**) và hỗ trợ xử lý **Code-switching Anh–Việt**.

---

# 1. Thử Thách Coding & Kiến Trúc Đa Luồng (Pseudo-code)

Giải pháp sử dụng:

* `collections.deque` làm **Ring Buffer** nhằm ngăn tràn RAM.
* `queue.Queue` kết hợp `threading` để triển khai mô hình **Producer (VAD) – Consumer (ASR)** bất đồng bộ.

Mã nguồn tham khảo: `src/always_on_pipeline.py`

## Pseudo-code

```python
import threading
import queue
import collections
import time


class AlwaysOnPipeline:
    def __init__(
        self,
        buffer_duration_sec=3,
        sample_rate=16000,
        chunk_size=512
    ):
        self.sample_rate = sample_rate
        self.chunk_size = chunk_size

        # KHỐI 1: Ring Buffer (Bộ đệm vòng)
        # Không dùng list() vô hạn.
        # maxlen giúp tự động ghi đè dữ liệu cũ,
        # tránh Memory Leak tuyệt đối.
        max_len = int(
            (buffer_duration_sec * sample_rate)
            / chunk_size
        )

        self.ring_buffer = collections.deque(
            maxlen=max_len
        )

        # KHỐI 2: Thread-safe Queue
        # Producer-Consumer
        # maxsize giúp chống Backpressure.
        self.audio_queue = queue.Queue(
            maxsize=50
        )

        self.is_running = False

        # Mock model loading
        # self.vad = load_silero_vad()
        # self.asr = load_sensevoice_small()

    def producer_audio_vad_thread(self):
        """
        Thread 1 (Ưu tiên cao, CPU thấp):
        Đọc microphone và chạy VAD liên tục.
        """

        while self.is_running:

            raw_chunk = self._read_from_mic(
                self.chunk_size
            )

            # Lưu vào Ring Buffer
            self.ring_buffer.append(
                raw_chunk
            )

            # Chạy VAD
            is_speech = self.vad.process(
                raw_chunk
            )

            if is_speech:
                try:
                    self.audio_queue.put(
                        raw_chunk,
                        timeout=0.05
                    )

                except queue.Full:
                    print(
                        "Cảnh báo: Queue đầy! "
                        "Kích hoạt Drop Frame."
                    )

    def consumer_asr_thread(self):
        """
        Thread 2 (Nặng CPU):
        Chỉ hoạt động khi Queue có dữ liệu.
        """

        while self.is_running:

            try:
                speech_chunk = (
                    self.audio_queue.get(
                        timeout=1.0
                    )
                )

                text = self.asr.transcribe(
                    speech_chunk
                )

                if text:
                    print(
                        f"[ASR Nhận lệnh]: {text}"
                    )

                self.audio_queue.task_done()

            except queue.Empty:
                continue

    def start_system(self):

        self.is_running = True

        t_producer = threading.Thread(
            target=self.producer_audio_vad_thread,
            daemon=True
        )

        t_consumer = threading.Thread(
            target=self.consumer_asr_thread,
            daemon=True
        )

        t_producer.start()
        t_consumer.start()
```

---

# 2. Câu Hỏi Giải Trình (Documentation)

## Câu 1: Xử lý Đa ngôn ngữ (Code-switching) trên mô hình TTS nhỏ

### Lựa chọn

Ưu tiên xử lý **Text Normalization bằng Regex/Rules** ở tầng phần mềm trước khi đưa dữ liệu vào mô hình TTS, thay vì nhúng bộ từ điển tiếng Anh (**Lexicon/G2P**) lớn vào tokenizer.

### Phân tích CPU/RAM

#### Ưu điểm

* Tiết kiệm tài nguyên tối đa.
* String Parsing và Regex trên ARM Cortex-A76 chỉ mất vài mili-giây.
* Gần như không tiêu tốn RAM bổ sung.
* Mô hình TTS (<100M tham số) không bị phình to.
* Giữ nguyên tốc độ suy luận (RTF).

Ví dụ ánh xạ thuật ngữ:

| Thuật ngữ   | Cách đọc    |
| ----------- | ----------- |
| BMS         | bi em ét    |
| CAN Bus     | can bớt     |
| Overcurrent | ô vờ cơ rần |

#### Nhược điểm

* Cần bảo trì bộ luật thủ công (Hard-code).
* Khó mở rộng nếu xuất hiện nhiều từ mới.

Tuy nhiên, bảng điều khiển xe điện là môi trường **Closed-domain**, tập từ vựng kỹ thuật tương đối cố định nên rủi ro OOV (Out-of-Vocabulary) thấp và hoàn toàn có thể kiểm soát bằng Rule-based Mapping.

---

## Câu 2: Kiểm soát Ngữ điệu (Prosody Control) Thời Gian Thực

### Giải pháp

Đối với các mô hình TTS hiện đại (ví dụ VITS), điều chỉnh trực tiếp các tham số:

* `length_scale` → tốc độ nói
* `noise_scale_w` → cao độ và độ biến thiên giọng

trong hàm `forward()` lúc inference.

### Tại sao không làm tăng độ trễ?

Khi giảm:

```python
length_scale = 0.75
```

thay vì:

```python
length_scale = 1.0
```

thì:

1. Duration Predictor sinh ít frame hơn.
2. Spectrogram ngắn hơn.
3. Vocoder xử lý ít phép toán hơn.
4. Âm thanh được sinh nhanh hơn.

### Kết quả

* Giọng nói khẩn cấp hơn.
* Cao độ cảnh báo rõ hơn.
* Không tăng inference time.
* Trong nhiều trường hợp còn giảm RTF so với cấu hình mặc định.

---

## Câu 3: Quản lý Hàng đợi & Backpressure

### Cơ chế bảo vệ

Hệ thống sử dụng đồng thời:

1. **Bounded Queue**
2. **Drop Frame**

```python
self.audio_queue = queue.Queue(
    maxsize=50
)
```

### Cách hoạt động

Nếu Producer (Mic + VAD) tạo dữ liệu nhanh hơn Consumer (ASR) xử lý:

```python
queue.Full
```

sẽ được kích hoạt.

Khi đó:

```python
except queue.Full:
    # Drop frame
```

hệ thống chủ động bỏ qua dữ liệu mới thay vì chặn luồng Producer.

### Lợi ích

* Không treo hệ thống.
* Không tăng RAM vô hạn.
* Bảo vệ CPU khỏi quá tải.
* Duy trì khả năng phản hồi thời gian thực.

### Tác động đến UX

Người dùng có thể gặp hiện tượng:

* Mất một vài từ.
* Nhận diện đứt quãng.
* Giảm độ chính xác trong môi trường cực kỳ ồn.

Tuy nhiên, trong hệ thống xe điện:

> Tính ổn định và an toàn hệ thống luôn quan trọng hơn việc giữ lại 100% dữ liệu âm thanh.

Việc Drop Frame giúp bảo vệ tài nguyên cho:

* Hệ thống điều khiển động cơ.
* Hệ thống phanh.
* Màn hình điều khiển trung tâm.

Qua đó tránh nguy cơ treo hệ thống hoặc vượt ngưỡng sử dụng CPU trong các tình huống vận hành quan trọng.

---

# Kết Luận

Kiến trúc đề xuất đáp ứng các mục tiêu:

* Always-on microphone.
* Tiêu thụ CPU thấp.
* Kiểm soát RAM chặt chẽ.
* Hỗ trợ Code-switching Anh–Việt.
* TTS thời gian thực có kiểm soát ngữ điệu.
* Chống Backpressure hiệu quả.
* Phù hợp triển khai trên Raspberry Pi 5 cho hệ thống trợ lý ảo xe điện.
