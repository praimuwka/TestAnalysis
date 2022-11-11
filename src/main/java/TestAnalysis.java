import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.linked.TIntLinkedList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class TestAnalysis {
//    private static Pattern quotes = Pattern.compile("\"([\\s\\S]+?)\"|\"\"");
//    private static Pattern quote = Pattern.compile("\"");
//    private static int count;

    public static void main(String[] args) {
        long m = System.currentTimeMillis();

        //получаем уникальные строки

        var rowSet = read(args[0]);

        //переносим в массив для быстрого доступа по индексу

        ArrayList<String> rowArr = new ArrayList<>(rowSet.size());
        for (String str: rowSet) {
            rowArr.add(str);
        }

        //выбираем валидные строки и определяем индексы для доступа к ним
        //
        //разбиваем строки на части
        //
        //находим число столбцов

        HashMap<String, Integer> rowMap = new HashMap(rowArr.size());
        ArrayList<String[]> vectorArr = new ArrayList(rowArr.size());
        int maxNumOfValues = 0;
        int index = 0;

        for(String row : rowArr){
            String[] vector = row.split(";");
            if(rowIsValid(vector)){ //здесь могла быть ваша валидация
                maxNumOfValues = Math.max(maxNumOfValues, vector.length);
                rowMap.put(row, index);
                index++;
                vectorArr.add(vector);
            }
        }
        int rowsCount = rowMap.size();

        // Создаем массив столбцов значений
        ArrayList<List<KV_SI_Struct>> columns = new ArrayList(maxNumOfValues);
        for (int i = 0; i < maxNumOfValues; i++) {
            columns.add(new ArrayList<KV_SI_Struct>());
        }

        // Раскидываем подстроки по столбцам
        for (int i = 0; i < rowsCount; i++) {
            String[] vector = vectorArr.get(i);
            for (int j = 0; j < vector.length; j++) {
                if (vector[j].length()>2) {
                    columns.get(j).add(new KV_SI_Struct(i, vector[j]));
                }
            }
        }


        //создаем список для подгрупп строк, которые должны быть объединены
        List<int[]> setsOfConnectedIndexes = new LinkedList();
        for (List<KV_SI_Struct> column : columns){
            //находим подгруппы в каждом столбце
            Map<String, List<KV_SI_Struct>> setsOfColumn = column.stream().
                    collect(Collectors.groupingBy(KV_SI_Struct::getString));
            //записываем подгруппы в список
            for (var elem : setsOfColumn.entrySet()){
                if(elem.getValue().size()>1){
                    int[] set = new int[elem.getValue().size()];
                    int i = 0;
                    for(KV_SI_Struct el: elem.getValue()){
                        set[i] = el.index;
                        i++;
                    }
                    setsOfConnectedIndexes.add(set);
                }
            }
        }

        //проходим по подгруппам и объединяем элементы в деревья (группы)

        QuickUnionPathHalvingUF uf = new QuickUnionPathHalvingUF(rowsCount);
        for(int[] setArr : setsOfConnectedIndexes){
            int papa = setArr[0];
            for (int i = 1; i < setArr.length; i++) {
                uf.union(papa, setArr[i]);
            }
        }

        //находим уникальный ключ для каждой группы + узнаем какой группе принадлежит каждая строка
        TIntSet groupKeysSet = new TIntHashSet();
        KV_II_Struct[] membersOfGroup = new KV_II_Struct[uf.parent.length];
        for (int i = 0; i < membersOfGroup.length; i++) {
            int papa = uf.find(uf.parent[i]);
            membersOfGroup[i] = new KV_II_Struct(papa, i);
            groupKeysSet.add(papa);
        }
//        for (int ii : parent){
//        }

        //для ключа каждой группы создаем контейнер, ключи и контейнеры выносим в отдельные массивы

        TIntIterator groupKeysSetIterator = groupKeysSet.iterator();
        TIntLinkedList[] listContainers = new TIntLinkedList[groupKeysSet.size()];
        int[] groupKeys = new int[groupKeysSet.size()];
        for (int i = 0; i < groupKeysSet.size(); i++) {
            TIntLinkedList list = new TIntLinkedList();
            listContainers[i] = list;
            groupKeys[i] = groupKeysSetIterator.next();
        }

        //находим, какой группе принадлежит каждая строка
//
//        KV_II_Struct[] membersOfGroup = new KV_II_Struct[parent.length];
//        for (int i = 0; i < membersOfGroup.length; i++) {
//            membersOfGroup[i] = new KV_II_Struct(uf.find(i), i);
//        }

        //группируем полученные key-value по ключу

        Map<Integer, List<KV_II_Struct>> groupsMap = Arrays.stream(membersOfGroup)
                .collect(Collectors.groupingBy(r->r.getKey()));

        //из полученных наборов формируем массивы индексов, входящих в одну группу

        TIntArrayList[] groupedIndexes = new TIntArrayList[groupsMap.keySet().size()];
        var groupsIterator = groupsMap.values().iterator();

        for (int i = 0; i < groupsMap.keySet().size(); i++) { //проход по спискам индексов (группам)
            var group = groupsIterator.next();
            TIntArrayList itemsIndexes = new TIntArrayList(group.size());
            group.stream().forEach(item->itemsIndexes.add(item.getValue()));
            groupedIndexes[i] = itemsIndexes;
        }

        // сортируем наборы по размеру

        Arrays.sort(groupedIndexes, Comparator.comparing(a->a.size()));

        //находим кол-во групп, в которые входит более 1 элемента

        int bigGroupsCount = 0;
        for (int i = 0; i < groupedIndexes.length; i++) {
            if(groupedIndexes[i].size()>1)
                bigGroupsCount++;
        }

        //выводим группы в файл по убыванию размера

        write("output.txt", groupedIndexes, bigGroupsCount, rowArr);

        System.out.println("Уникальных строк: " + rowsCount);
        System.out.println("Групп: " + groupsMap.size());
        System.out.println("Больших групп: " + bigGroupsCount);
        System.out.println("Время выполнения: " + ((System.currentTimeMillis()-m)/1000 + 1) + " сек");
    }
    public static boolean rowIsValid(String[] parts){
        //
        // все значения были предварительно проанализированы
        // невалидных строк не найдено
        // поэтому здесь пусто
        // при необходимости можно добавить валидацию в этот метод
        //
        return true;
    }
    public static HashSet<String> read(String path){
        HashSet<String> set = new HashSet(1000000, 0.99f);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            while (line != null) {
                set.add(line);
                line = reader.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return set;
    }

    public static void write(String path, TIntArrayList[] groups, int bigGroupsCount, ArrayList<String> strings){
        try {
            Path newFilePath = Paths.get(path);
            Files.deleteIfExists(newFilePath);
            var x = Files.createFile(newFilePath);
            var sep = System.getProperty("line.separator");
            FileWriter writer = new FileWriter(x.toFile());
            int groupCounter = 0;
            writer.write("Групп с более чем одним элементом: " +  bigGroupsCount + sep + sep);
            for (int i = groups.length-1; i >=0; i--) {
                writer.write("Группа " + ++groupCounter + sep + sep);
                var group = groups[i];
                for (int j = 0; j < group.size(); j++) {
                    writer.write(strings.get(group.get(j)) + sep + sep);
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
class KV_SI_Struct {
    int index;
    String string;

    public KV_SI_Struct(int index, String string) {
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
class KV_II_Struct {
    int key;
    int value;

    public KV_II_Struct(int key, int value) {
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