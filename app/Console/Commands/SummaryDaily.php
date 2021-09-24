<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class SummaryDaily extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'summary:daily';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Command description';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle()
    {
        set_time_limit(0);
        $data = [];
        $dateNow = date("Y-m-d");
        $proyekList = DB::select("WITH pekerja_backlist AS (
            SELECT A.proyek_id, COUNT(A.id) AS total FROM m_pekerja_proyek A
        INNER JOIN m_pekerja B ON A.pekerja_id = B.id AND B.blacklist = 'Y'
        GROUP BY A.proyek_id
        ), pekerja_non_active AS (
            SELECT A.proyek_id, COUNT(A.id) AS total FROM m_pekerja_proyek A
            INNER JOIN m_pekerja B ON A.pekerja_id = B.id AND (B.blacklist <> 'Y' OR B.blacklist IS NULL)
                AND B.active = 'N'
            GROUP BY A.proyek_id
        ), pekerja_active AS (
            SELECT A.proyek_id, COUNT(A.id) AS total FROM m_pekerja_proyek A
            INNER JOIN m_pekerja B ON A.pekerja_id = B.id AND (B.blacklist <> 'Y' OR B.blacklist IS NULL)
                AND B.active = 'Y'
            GROUP BY A.proyek_id
        ), pekerja_total AS (
            SELECT B.proyek_id, COUNT(1) AS total FROM m_pekerja A
        INNER JOIN m_pekerja_proyek B ON B.pekerja_id = A.id
        GROUP BY B.proyek_id
        ), presensi_total AS (
            SELECT COUNT(1) AS total, proyek_id FROM r_presensi
        WHERE SUBSTR(created_at,1,8) = '" . $dateNow . "' AND flg_in_out = 'I'
        GROUP BY proyek_id, SUBSTR(created_at,1,8)
        ), presensi_terlambat AS (
            SELECT COUNT(1) AS total, proyek_id FROM r_presensi
        WHERE SUBSTR(created_at,1,8) = '" . $dateNow . "' AND flg_terlambat = 'Y'
        GROUP BY proyek_id, SUBSTR(created_at,1,8)
        ), presensi_lembur AS (
            SELECT COUNT(1) AS total, proyek_id FROM r_presensi
        WHERE SUBSTR(created_at,1,8) = '" . $dateNow . "' AND flg_lembur = 'Y'
        GROUP BY proyek_id, SUBSTR(created_at,1,8)
        ), pekerja_normal AS (
            SELECT COUNT(1) AS total, B.proyek_id FROM m_pekerja A
        INNER JOIN m_pekerja_proyek B ON B.pekerja_id = A.id AND A.status_bekerja_id = 1
        GROUP BY B.proyek_id
        ), pekerja_odp AS (
            SELECT COUNT(1) AS total, B.proyek_id FROM m_pekerja A
        INNER JOIN m_pekerja_proyek B ON B.pekerja_id = A.id AND A.status_bekerja_id = 2
        GROUP BY B.proyek_id
        ), pekerja_tms AS (
            SELECT COUNT(1) AS total, B.proyek_id FROM m_pekerja A
        INNER JOIN m_pekerja_proyek B ON B.pekerja_id = A.id AND A.status_bekerja_id = 5
        GROUP BY B.proyek_id
        ), pekerja_covid AS (
            SELECT A.proyek_id, COUNT(1) AS total FROM m_pekerja_proyek A
                INNER JOIN m_pekerja B ON B.id = A.pekerja_id AND flg_covid = 'Y'
                INNER JOIN m_proyek C ON C.id = A.proyek_id
            GROUP BY A.proyek_id
        ), pekerja_belum_covid AS (
            SELECT A.proyek_id, COUNT(1) AS total FROM m_pekerja_proyek A
                INNER JOIN m_pekerja B ON B.id = A.pekerja_id AND flg_covid = 'N'
                INNER JOIN m_proyek C ON C.id = A.proyek_id
            GROUP BY A.proyek_id
        ), pekerja_vaksin AS (
            SELECT A.proyek_id, COUNT(1) AS total FROM m_pekerja_proyek A
                INNER JOIN m_pekerja B ON B.id = A.pekerja_id AND vaksin = 'Y'
                INNER JOIN m_proyek C ON C.id = A.proyek_id
            GROUP BY A.proyek_id
        ), pekerja_belum_vaksin AS (
            SELECT A.proyek_id, COUNT(1) AS total FROM m_pekerja_proyek A
                INNER JOIN m_pekerja B ON B.id = A.pekerja_id AND vaksin = 'N'
                INNER JOIN m_proyek C ON C.id = A.proyek_id
            GROUP BY A.proyek_id
        )
        SELECT COALESCE(B.total, 0) AS total_backlist,
        COALESCE(C.total, 0) AS total_non_active,
        COALESCE(D.total, 0) AS total_active,
        COALESCE(E.total, 0) AS total,
        COALESCE(F.total, 0) AS total_presensi,
        COALESCE(G.total, 0) AS total_terlambat,
        COALESCE(H.total, 0) AS total_lembur,
        COALESCE(I.total, 0) AS total_pekerja_normal,
        COALESCE(J.total, 0) AS total_pekerja_odp,
        COALESCE(K.total, 0) AS total_pekerja_tms,
        COALESCE(L.total, 0) AS total_pekerja_covid,
        COALESCE(M.total, 0) AS total_pekerja_belum_covid,
        COALESCE(N.total, 0) AS total_pekerja_vaksin,
        COALESCE(O.total, 0) AS total_pekerja_belum_vaksin, A.id, A.nama_pendek_proyek, A.nama_proyek
        FROM m_proyek A
        LEFT JOIN pekerja_backlist B ON B.proyek_id = A.id
        LEFT JOIN pekerja_non_active C ON C.proyek_id = A.id
        LEFT JOIN pekerja_active D ON D.proyek_id = A.id
        LEFT JOIN pekerja_total E ON E.proyek_id = A.id
        LEFT JOIN presensi_total F ON F.proyek_id = A.id
        LEFT JOIN presensi_terlambat G ON G.proyek_id = A.id
        LEFT JOIN presensi_lembur H ON H.proyek_id = A.id
        LEFT JOIN pekerja_normal I ON I.proyek_id = A.id
        LEFT JOIN pekerja_odp J ON J.proyek_id = A.id
        LEFT JOIN pekerja_tms K ON K.proyek_id = A.id
        LEFT JOIN pekerja_covid L ON L.proyek_id = A.id
        LEFT JOIN pekerja_belum_covid M ON M.proyek_id = A.id
        LEFT JOIN pekerja_vaksin N ON N.proyek_id = A.id
        LEFT JOIN pekerja_belum_vaksin O ON O.proyek_id = A.id");
        foreach ($proyekList as $proyek) {
            $proyek->summary = DB::select("WITH summary AS (
                SELECT A.tipe_pekerja_id, COUNT(1) AS total
                FROM m_pekerja_proyek A
                INNER JOIN m_proyek C ON C.id = A.proyek_id AND proyek_id = :id
                GROUP BY A.tipe_pekerja_id
            ) SELECT A.id, A.nama_tipe_pekerja, COALESCE(B.total, 0) AS total
                FROM m_tipe_pekerja A
                LEFT JOIN summary B ON B.tipe_pekerja_id = A.id
                ORDER BY A.id", [
                "id" => $proyek->id
            ]);
            file_put_contents(public_path("rekap/" . $proyek->id . ".json"), json_encode($proyek));
            file_put_contents(public_path("rekap/" . $proyek->nama_pendek_proyek . ".json"), json_encode($proyek));
        }
        file_put_contents(public_path("rekap/proyek.json"), json_encode($proyekList));
    }
}
