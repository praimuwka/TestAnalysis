import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TestAnalysis {
    private static final Pattern quotes = Pattern.compile("\"([\\s\\S]+?)\"|\"\"");

    public static void main(String[] args) {
        long m = System.currentTimeMillis();

        //получаем уникальные строки

        HashSet<String> rowSet = read(args[0]);

        //массивы для быстрого доступа по индексу
        ArrayList<String> validRows = new ArrayList(rowSet.size());
        ArrayList<String[]> vectorArr = new ArrayList(rowSet.size());

        //разбиваем строки на части
        //
        //выбираем валидные строки и определяем индексы для доступа к ним
        //
        //находим число столбцов

        int maxNumOfValues = 0;
        for(String row : rowSet){
            String[] vector = row.split(";");
            if(rowIsValid(vector)){
                maxNumOfValues = Math.max(maxNumOfValues, vector.length);
                vectorArr.add(vector);
                validRows.add(row);
            }
        }

        // Создаем массив столбцов значений

        ArrayList<List<KV_Si_Struct>> columns = new ArrayList(maxNumOfValues);
        for (int i = 0; i < maxNumOfValues; i++) {
            columns.add(new ArrayList<KV_Si_Struct>());
        }

        // Распределяем подстроки по столбцам

        for (int i = 0; i < validRows.size(); i++) {
            String[] vector = vectorArr.get(i);
            for (int j = 0; j < vector.length; j++) {
                if (vector[j].length()>2) {
                    columns.get(j).add(new KV_Si_Struct(i, vector[j]));
                }
            }
        }

        //находим подгруппы, и объединяем элементы

        QuickUnionWithpathComressionUF uf = new QuickUnionWithpathComressionUF(validRows.size());
        int alikeNumber = 0;
        for (List<KV_Si_Struct> column : columns){
            Map<String, List<KV_Si_Struct>> setsToUnion = column.stream().
                    collect(Collectors.groupingBy(KV_Si_Struct::getString));
            for (var set: setsToUnion.values()){
                if(set.size()>1){
                    alikeNumber += set.size();
                    int papa = uf.find(set.get(0).getIndex());
                    for (int i = 1; i < set.size(); i++) {
                        uf.union(papa, set.get(i).getIndex());
                    }
                }
            }
        }

        //компануем группы по ключу, сортируем по размеру и собираем в массив

        int[] indexes = new int[uf.parent.length];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i]=i;
        }
        var groups = Arrays.stream(indexes).boxed()
                .collect(Collectors.groupingBy(x->uf.find(uf.parent[x])))
                .values().stream()
                .sorted(Comparator.comparingInt(x -> x.size()))
                .collect(Collectors.toCollection(ArrayList::new));

        //находим кол-во групп, в которые входит более 1 элемента

        long bigGroupsCount = 0;
        int groupIndex = groups.size()-1;
        while(groupIndex >= 0 && groups.get(groupIndex--).size()>1){
            bigGroupsCount++;
        }

        //выводим группы в файл по убыванию размера

        write("output.txt", groups, bigGroupsCount, validRows);

        //вывод статистики

        System.out.println("Время выполнения: \t\t\t"
                + String.format("%.2f",((System.currentTimeMillis()-m)*1.0/1000)) + " сек");
        System.out.println("Групп: \t\t\t\t\t\t" + groups.size());
        System.out.println("Больших групп: \t\t\t\t" + bigGroupsCount);
        System.out.println("Уникальных валидных строк: \t" + validRows.size());
    }
    public static boolean rowIsValid(String[] parts){
        for (String s : parts){
            if(!quotes.matcher(s).matches()&&s!="")
                return false;
        }
        return true;
    }
    public static HashSet<String> read(String path){
        HashSet<String> rows = new HashSet();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            while (line != null) {
                rows.add(line);
                line = reader.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rows;
    }

    public static void write(String path, ArrayList<List<Integer>> groups, long bigGroupsCount, ArrayList<String> strings){
        try {
            Path newFilePath = Paths.get(path);
            Files.deleteIfExists(newFilePath);
            Path x = Files.createFile(newFilePath);
            String sep = System.getProperty("line.separator");
            FileWriter writer = new FileWriter(x.toFile());
            int groupCounter = 0;
            writer.write("Групп с более чем одним элементом: " +  bigGroupsCount + sep + sep);
            for (int i = groups.size()-1; i >=0; i--) {
                writer.write("Группа " + ++groupCounter + sep + sep);
                List<Integer> group = groups.get(i);
                for (var item : group) {
                    writer.write(strings.get(item) + sep + sep);
                }
            }
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
//структура строка-индекс String-int для группировки по строке при помощи Stream api
class KV_Si_Struct {
    int index;
    String string;

    public KV_Si_Struct(int index, String string) {
        this.index = index;
        this.string = string;
    }

    public int getIndex() {
        return index;
    }

    public String getString() {
        return string;
    }
}
//структура ключ-значение int-int для группировки по ключу при помощи Stream api
class KV_ii_Struct {
    int key;
    int value;

    public KV_ii_Struct(int key, int value) {
        this.key = key;
        this.value = value;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }
}