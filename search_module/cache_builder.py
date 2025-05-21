import subprocess
import sys
import os
import time
import logging

# --- Loglama Yapılandırması ---
logging.basicConfig(
    filename='cache_builder.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def build_search_cache(
    word_dataset_file="turkish_word_dataset.txt",
    search_script="search.py"
):
    """
    turkish_word_dataset.txt dosyasındaki her kelimeyi alıp
    search.py betiğine parametre olarak göndererek arama yapar ve önbellek oluşturur.
    """
    
    if not os.path.exists(word_dataset_file):
        logging.error(f"HATA: '{word_dataset_file}' dosyası bulunamadı. Lütfen dosya adını ve yolunu kontrol edin.")
        print(f"HATA: '{word_dataset_file}' dosyası bulunamadı. Lütfen dosya adını ve yolunu kontrol edin.")
        sys.exit(1)

    if not os.path.exists(search_script):
        logging.error(f"HATA: '{search_script}' betiği bulunamadı. Lütfen dosya adını ve yolunu kontrol edin.")
        print(f"HATA: '{search_script}' betiği bulunamadı. Lütfen dosya adını ve yolunu kontrol edin.")
        sys.exit(1)

    processed_words_count = 0
    start_time = time.time()
    
    print(f"'{word_dataset_file}' dosyasındaki kelimeler taranıyor ve '{search_script}' çalıştırılıyor...")
    logging.info(f"'{word_dataset_file}' dosyasındaki kelimeler taranıyor ve '{search_script}' çalıştırılıyor...")

    try:
        with open(word_dataset_file, "r", encoding="utf-8") as f:
            all_words = [line.strip() for line in f if line.strip()]
        
        total_words = len(all_words)
        print(f"Toplam {total_words} kelime bulundu. İşleme başlanıyor...")
        logging.info(f"Toplam {total_words} kelime bulundu. İşleme başlanıyor...")

        for i, word in enumerate(all_words):
            processed_words_count += 1
            print(f"[{processed_words_count}/{total_words}] '{word}' kelimesi için arama başlatılıyor...")
            logging.info(f"[{processed_words_count}/{total_words}] '{word}' kelimesi için arama başlatılıyor.")
            
            # search.py betiğini subprocess ile çalıştır
            # capture_output=False ile search.py'nin kendi çıktısını doğrudan terminale yazmasını sağlarız.
            try:
                # Python betiğini çalıştırmak için sys.executable kullanmak daha güvenlidir.
                result = subprocess.run(
                    [sys.executable, search_script, word],
                    check=True, # Komut hata koduyla çıkarsa CalledProcessError fırlatır
                    capture_output=False, # search.py çıktısını doğrudan terminale yönlendirir
                    text=True, # Metin tabanlı çıktı için
                    encoding="utf-8" # Çıktı kodlaması
                )
                logging.info(f"'{word}' kelimesi için arama tamamlandı. Çıkış kodu: {result.returncode}")
            except subprocess.CalledProcessError as e:
                logging.error(f"HATA: '{word}' kelimesi için '{search_script}' çalıştırılırken hata oluştu. Çıkış kodu: {e.returncode}")
                logging.error(f"Komut çıktısı:\n{e.stdout}\n{e.stderr}")
                print(f"HATA: '{word}' kelimesi için '{search_script}' çalıştırılırken hata oluştu. Log dosyasına bakın.")
                # Hata olsa bile diğer kelimelerle devam etmek için break kullanmıyoruz
            except FileNotFoundError:
                logging.error(f"HATA: '{search_script}' komutu bulunamadı. Python yolunuzu kontrol edin veya betiğin doğru yerde olduğundan emin olun.")
                print(f"HATA: '{search_script}' komutu bulunamadı.")
                sys.exit(1) # Kritik hata, betiği sonlandır
            except Exception as e:
                logging.error(f"Beklenmedik hata: '{word}' kelimesi için '{search_script}' çalıştırılırken: {e}")
                print(f"Beklenmedik hata oluştu: {e}. Log dosyasına bakın.")

            # Her arama arasında kısa bir bekleme ekleyebiliriz,
            # bu sayede MongoDB veya diğer sistemler aşırı yüklenmez.
            # time.sleep(0.1) # 100 milisaniye bekleme

    except Exception as e:
        logging.critical(f"Genel bir hata oluştu: {e}")
        print(f"Genel bir hata oluştu: {e}. Lütfen log dosyasına bakın.")
        sys.exit(1)

    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\nİşlem tamamlandı! Toplam {processed_words_count} kelime işlendi.")
    print(f"Geçen süre: {duration:.2f} saniye.")
    logging.info(f"İşlem tamamlandı. Toplam {processed_words_count} kelime işlendi. Geçen süre: {duration:.2f} saniye.")
    print("Önbellekleme süreci bitti.")

# Betiği çalıştırmak için
if __name__ == "__main__":
    # turkish_word_dataset.txt ve search.py dosyalarının bu betikle aynı dizinde olduğunu varsayıyoruz.
    build_search_cache()