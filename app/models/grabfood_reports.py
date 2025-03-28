from app.extensions import db

class GrabFoodReport(db.Model):
    __tablename__ = 'grabfood_reports'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    brand_name = db.Column(db.String, nullable=False)
    outlet_code = db.Column(db.String, nullable=False)
    # nama_merchant = db.Column(db.String, nullable=True)
    # id_merchant = db.Column(db.String, nullable=True)
    nama_toko = db.Column(db.String, nullable=True)
    id_toko = db.Column(db.String, nullable=False)
    diperbarui_pada = db.Column(db.DateTime, nullable=True)
    tanggal_dibuat = db.Column(db.DateTime, nullable=True)
    jenis = db.Column(db.String, nullable=True)
    kategori = db.Column(db.String, nullable=True)
    subkategori = db.Column(db.String, nullable=True)
    status = db.Column(db.String, nullable=True)
    id_transaksi = db.Column(db.String, nullable=True, unique=True)
    # id_transaksi_dihubungkan = db.Column(db.String, nullable=True)
    # id_transaksi_partner_1 = db.Column(db.String, nullable=True)
    # id_transaksi_partner_2 = db.Column(db.String, nullable=True)
    id_pesanan_panjang = db.Column(db.String, nullable=True)
    id_pesanan_pendek = db.Column(db.String, nullable=True)
    kode_booking = db.Column(db.String, nullable=True)
    saluran_pesanan = db.Column(db.String, nullable=True)
    jenis_pesanan = db.Column(db.String, nullable=True)
    metode_pembayaran = db.Column(db.String, nullable=True)
    # id_terminal = db.Column(db.String, nullable=True)
    # saluran = db.Column(db.String, nullable=True)
    tipe_promo = db.Column(db.String, nullable=True)
    # biaya_grab_persen = db.Column(db.Numeric(10, 2), nullable=True)
    # pengali_poin = db.Column(db.Numeric(10, 2), nullable=True)
    # poin_diberikan = db.Column(db.Numeric(10, 2), nullable=True)
    id_pencairan_dana = db.Column(db.String, nullable=True)
    tanggal_transfer = db.Column(db.DateTime, nullable=True)
    amount = db.Column(db.Numeric(10, 2), nullable=True)
    # pajak_atas_pesanan = db.Column(db.Numeric(10, 2), nullable=True)
    biaya_kemasan = db.Column(db.Numeric(10, 2), nullable=True)
    # biaya_pelanggan_tidak_ikut_keanggotaan = db.Column(db.Numeric(10, 2), nullable=True)
    # biaya_layanan_restoran = db.Column(db.Numeric(10, 2), nullable=True)
    # promo = db.Column(db.Numeric(10, 2), nullable=True)
    diskon_dibiayai_merchant = db.Column(db.Numeric(10, 2), nullable=True)
    diskon_ongkos_kirim_dibiayai_merchant = db.Column(db.Numeric(10, 2), nullable=True)
    # ongkos_kirim_ditanggung_merchant_online = db.Column(db.Numeric(10, 2), nullable=True)
    # ongkos_kirim_ditanggung_merchant_pengantaran = db.Column(db.Numeric(10, 2), nullable=True)
    # biaya_layanan_pengiriman_grabexpress = db.Column(db.Numeric(10, 2), nullable=True)
    penjualan_bersih = db.Column(db.Numeric(10, 2), nullable=True)
    # nilai_mdr_bersih = db.Column(db.Numeric(10, 2), nullable=True)
    # pajak_mdr = db.Column(db.Numeric(10, 2), nullable=True)
    # biaya_grab = db.Column(db.Numeric(10, 2), nullable=True)
    biaya_sukses_pemasaran = db.Column(db.Numeric(10, 2), nullable=True)
    # komisi_pengantaran = db.Column(db.Numeric(10, 2), nullable=True)
    # komisi_saluran = db.Column(db.Numeric(10, 2), nullable=True)
    komisi_pesanan = db.Column(db.Numeric(10, 2), nullable=True)
    komisi_lain_grabfood_grabmart = db.Column(db.Numeric(10, 2), nullable=True)
    komisi_grabkitchen = db.Column(db.Numeric(10, 2), nullable=True)
    komisi_lain_grabkitchen = db.Column(db.Numeric(10, 2), nullable=True)
    pajak_pemotongan = db.Column(db.Numeric(10, 2), nullable=True)
    total = db.Column(db.Numeric(10, 2), nullable=True)
    # pajak_atas_mdr_persen = db.Column(db.Numeric(10, 2), nullable=True)
    # komisi_pengantaran_persen = db.Column(db.Numeric(10, 2), nullable=True)
    # komisi_saluran_persen = db.Column(db.Numeric(10, 2), nullable=True)
    # komisi_pesanan_persen = db.Column(db.Numeric(10, 2), nullable=True)
    pajak_atas_komisi_grabfood_grabmart = db.Column(db.Numeric(10, 2), nullable=True)
    penyesuaian_iklan = db.Column(db.Numeric(10, 2), nullable=True)
    pajak_atas_total_komisi_grabkitchen = db.Column(db.Numeric(10, 2), nullable=True)
    alasan_pembatalan = db.Column(db.String, nullable=True)
    dibatalkan_oleh = db.Column(db.String, nullable=True)
    alasan_pengembalian_dana = db.Column(db.String, nullable=True)
    deskripsi = db.Column(db.String, nullable=True)
    kelompok_insiden = db.Column(db.String, nullable=True)
    nama_insiden = db.Column(db.String, nullable=True)
    # item_terdampak = db.Column(db.String, nullable=True)
    # link_untuk_banding = db.Column(db.String, nullable=True)
    # status_banding = db.Column(db.String, nullable=True)

    def __repr__(self):
        return f"<GrabFoodReport {self.id_transaksi}, {self.tanggal_dibuat}>"