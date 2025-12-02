package handler;

import model.Person;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class Handler implements Callable<List<Person>> {

    private final String filePath;
    private final int startRow;
    private final int endRow;

    public Handler(String filePath, int startRow, int endRow) {
        this.filePath = filePath;
        this.startRow = startRow;
        this.endRow = endRow;
    }

    @Override
    public List<Person> call() throws Exception {
        try (FileInputStream fileInputStream = new FileInputStream(new File(filePath));
             XSSFWorkbook workbook = new XSSFWorkbook(fileInputStream)) {

            XSSFSheet sheet = workbook.getSheetAt(0);
            List<Person> users = new ArrayList<>();
            DataFormatter dataFormatter = new DataFormatter();

            int actualEndRow = Math.min(endRow, sheet.getPhysicalNumberOfRows());

            for (int n = startRow; n < actualEndRow; n++) {
                Row row = sheet.getRow(n);

                if (row == null) continue;

                Person user = new Person();
                int i = row.getFirstCellNum();

                user.setId((long) row.getCell(i).getNumericCellValue());
                user.setFirst_name(row.getCell(++i).getStringCellValue());
                user.setLast_name(row.getCell(++i).getStringCellValue());
                user.setEmail(row.getCell(++i).getStringCellValue());
                user.setGender(row.getCell(++i).getStringCellValue());
                user.setIp_address(row.getCell(++i).getStringCellValue());
                user.setCity(row.getCell(++i).getStringCellValue());
                user.setBank(row.getCell(++i).getStringCellValue());

                users.add(user);
            }
            return users;
        }
    }
}