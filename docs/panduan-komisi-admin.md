# Panduan komisi outlet untuk admin

**Acuan: 26 November 2026.** Pilih merek outlet, platform, lalu jenis laporan. *Net* adalah nilai bersih dari laporan platform. *Pencairan* adalah transfer yang dilaporkan platform; *mutasi* adalah uang yang tercatat di rekening. Kolom **(ac)** menunjukkan pendapatan **setelah komisi**, bukan jumlah potongannya. Jangan menjumlahkan komisi dari beberapa laporan untuk transaksi yang sama.

## Harian dan closing standar

Kelompok **MPR** meliputi MPR, MPR Mandiri, dan MPR Non MP78. Jika laporan outlet juga memuat outlet MPR pasangannya, tarif MPR hanya berlaku pada bagian MPR.

| Platform | MPR | MP78, MP78 Express, dan non-MPR lainnya | Dasar perhitungan |
|---|---|---|---|
| Gojek | 8% GoFood; 0% QRIS | 1%, termasuk QRIS | MPR: Gojek Net dipisahkan menjadi GoFood dan QRIS. Non-MPR: mutasi, atau Gojek Net bila mutasi kosong atau nol. |
| Grab | 8% GrabFood; 0% OVO | MP78: 1/74; lainnya (termasuk MP78 Express): 1%. Keduanya termasuk OVO. | Pencairan selesai, atau Grab Net bila tidak ada pencairan. Pilih per hari. |
| ShopeeFood | 8% | 0% | Mutasi ShopeeFood, atau net bila mutasi kosong atau nol. |
| ShopeePay | 8% | 0% | Mutasi ShopeePay, atau net bila mutasi kosong atau nol. Tarif bulanan MPR berbeda. |
| TikTok | 5% | 5% | Net pesanan. Kolom pencairan TikTok hanya pembanding. |
| Qpon | 1% | 1% | Harian: nilai tagihan sebelum potongan. Closing: mutasi yang cocok pada hari itu; bila tidak ada, gunakan nilai tagihan. |
| Webshop | 3% saat impor; tambahan 8% pada kolom harian *Webshop Net (ac)* | 3% saat impor | Dari *Net Order Value* pesanan selesai Rp100.000, Webshop Net menjadi Rp97.000. Closing memakai Rp97.000; kolom harian (ac) MPR menunjukkan Rp89.240. |
| Maxim | 0% | 0% | *Net Order Value* pesanan selesai diterima utuh. |

**Pengecualian outlet dan tanggal:**

- **Es Ce Hun Tiau & Bongko Wendy:** tidak ada kolom harian (ac). Closing tidak memotong komisi Gojek, Grab, Shopee, atau Qpon. Nilai TikTok di closing tetap dipotong 5%; Webshop memakai net setelah potongan saat impor.
- **Pukis & Martabak Kota Baru:** *Closing Pukis* tidak memotong komisi tambahan. Gojek memakai mutasi atau net; Grab memakai pencairan atau net; Shopee dan TikTok memakai net; Qpon memakai mutasi yang cocok atau nilai tagihan. Di lembar harian, Gojek (ac) dan Qpon (ac) masing-masing memakai 1%; Grab tampil tanpa kolom (ac).
- **MP78, Martabak 777 Sinar Bulan, dan Martabak 999 Asli Bandung:** kolom *Grab (net)* pada closing menunjukkan nilai sebelum komisi; komisi Grab dicatat sebagai pengeluaran.
- **TikTok dan Qpon:** untuk MP78 dan kelompok MPR, closing mencatat pesanan TikTok dan tagihan Qpon **7 hari setelah tanggal asal**. Merek lain tidak memakai pergeseran ini.

## Contoh: kapan memakai Grab Net?

Untuk setiap hari transaksi, gunakan jumlah pencairan selesai outlet untuk transfer **hari berikutnya (H+1)**. Jika pencairan tercatat Rp0, gunakan Rp0. Jika **tidak ada** pencairan selesai yang cocok, gunakan Grab Net. Mutasi bank kosong tidak membatalkan pencairan yang sudah tercatat.

| Contoh MPR | Grab Net | Pencairan | OVO | Komisi GrabFood | Pendapatan setelah komisi |
|---|---:|---:|---:|---:|---:|
| Hari 1: ada pencairan | Rp1.000.000 | Rp950.000 | Rp150.000 | 8% × (Rp950.000 − Rp150.000) = Rp64.000 | Rp886.000 |
| Hari 2: belum ada pencairan | Rp500.000 | — | Rp100.000 | 8% × (Rp500.000 − Rp100.000) = Rp32.000 | Rp468.000 |

Komisi MPR dua hari itu Rp96.000. Pada closing MP78, OVO tidak dipisahkan: komisi untuk angka yang sama adalah **(Rp950.000 + Rp500.000) / 74 ≈ Rp19.595**.

## Contoh: GoFood MPR dengan QRIS

Jika Gojek Net Rp1.000.000 sudah termasuk QRIS Rp200.000, bagian GoFood adalah Rp800.000. Komisi **8% × Rp800.000 = Rp64.000**. QRIS diterima penuh, jadi **GoFood x QRIS (ac) = Rp736.000 + Rp200.000 = Rp936.000**. Jika seluruh nilai adalah QRIS, komisinya nol.

Gunakan **GoFood x QRIS (ac)** untuk mencocokkan closing MPR. Kolom harian **Gojek Net Mutation (ac)** menunjukkan 92% dari seluruh mutasi (atau Gojek Net bila mutasi kosong atau nol), tanpa memisahkan QRIS. Dalam contoh ini, tanpa mutasi, kolom itu menunjukkan Rp920.000. Kedua kolom bukan dua pendapatan terpisah.

## Ringkasan (*Summary*)

Bagian komisi khusus muncul untuk **MP78, MP78 Express, Martabak 777 Sinar Bulan, Martabak 999 Asli Bandung, Martabak Surya Kencana, dan Martabak Akim**.

| Baris ringkasan | Komisi |
|---|---|
| Management Commission (GrabFood) | MP78: 1/74; lainnya: 1% dari (nilai Grab terpilih − OVO). Hanya potongan ini yang mengurangi total pendapatan online pada Summary. |
| Partner Commission (GrabFood) | Tarif dan dasar GrabFood sama dengan komisi manajemen, ditampilkan terpisah. |
| Management Commission (TikTok) | **1% dari TikTok Net**. Untuk net Rp100.000, komisi Summary Rp1.000. Tarif TikTok di closing dan laporan bulanan tetap 5%. |

**Rekap total komisi Grab MP78/MP78 Express** menggabungkan kedua merek dan memakai transaksi Grab, bukan pencairan harian. Komisi manajemen dan mitra masing-masing memakai **1/74 untuk transaksi MP78** dan **1% untuk transaksi MP78 Express**. Dengan tanggal awal dan akhir lengkap, hanya GrabFood yang dihitung; tanpa rentang lengkap, jenis transaksi tidak dibatasi.

## Laporan komisi bulanan

Rekap bulanan memakai transaksi platform, bukan mutasi atau pencairan harian. **—** berarti platform tidak dicantumkan; **0%** berarti pendapatan dicantumkan tanpa potongan.

| Platform | Komisi MPR bulanan | Komisi Manajemen non-MPR bulanan / rentang tanggal |
|---|---|---|
| Gojek | 8% transaksi non-QRIS; 0% QRIS | **1% dari Gojek Net**, termasuk QRIS |
| Grab | 8% transaksi selain OVO; 0% OVO | MP78: 1/74; lainnya: 1% dari total transaksi Grab, termasuk OVO |
| ShopeeFood | 8% dari pendapatan bersih; pesanan batal tidak dihitung | — |
| ShopeePay | 0% dari nilai penyelesaian transaksi; penarikan saldo tidak dihitung | — |
| TikTok | 5% dari net pesanan | 5% dari net pesanan |
| Qpon | — | 1% dari nilai tagihan sebelum potongan |
| Webshop | — | 3% dari Webshop Net setelah impor; dari Rp97.000, komisi bulanan Rp2.910 |
| Maxim | 0% | 0% |

Sebagai contoh, jika transaksi dan mutasi ShopeePay MPR sama-sama Rp100.000, closing menunjukkan Rp92.000 setelah komisi. Pada laporan komisi bulanan MPR, nilainya tetap Rp100.000.
