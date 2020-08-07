import logging
import os
import sys
import time
import re
import traceback
import configparser
from tkinter import messagebox, filedialog

import pandas as pd

logger = logging.getLogger('SyncLogger')
pd.options.mode.chained_assignment = None


def read_excel(excel_file_path):
    df = pd.read_excel(excel_file_path, na_filter=False)
    return df


def write_result(output_path, dataframe, tsv):
    dataframe.QnaId = dataframe.QnaId.astype(int)
    dataframe.IsContextOnly = dataframe.IsContextOnly.astype('bool')
    dataframe.fillna('')

    dataframe.to_excel(output_path, index=False)

    if tsv:
        dataframe.to_csv(output_path.replace('xlsx', 'tsv'), sep='\t', index=False)


def insert_row(row_number, df, row_value):
    df1 = df.iloc[0:row_number]
    df2 = df.iloc[row_number:]
    df1.loc[row_number] = row_value

    df_result = pd.concat([df1, df2])
    df_result.index = [*range(df_result.shape[0])]

    return df_result


def text_cleaner(text):
    clean_text = text.lower()

    re_half = re.compile(r'[!-/:-@[-`{-~]')
    re_full = re.compile(r'[︰-＠]')
    re_full2 = re.compile(r'[、・’〜：＜＞＿｜「」｛｝【】『』〈〉“”◯○〔〕…――――◇]')
    re_comma = re.compile(r'[。]')
    re_n = re.compile(r'\\n')
    re_space = re.compile(r'[\s+]')
    re_dash = re.compile(r'\\')

    clean_text = re_n.sub("", clean_text)
    clean_text = re_half.sub("", clean_text)
    clean_text = re_full.sub("", clean_text)
    clean_text = re_space.sub("", clean_text)
    clean_text = re_full2.sub("", clean_text)
    clean_text = re_comma.sub("", clean_text)
    clean_text = re_dash.sub("", clean_text)

    return clean_text


def remove_prefix_suffix(text, white_list):
  clean_text = 'not_defined'

  for item in white_list:
    if item in text:
      clean_text = item
      break

  return clean_text


def white_list_gen():
    config = configparser.ConfigParser()
    relative_path = 'config.ini'
    base_path = os.path.abspath(".")
    config_path = os.path.join(base_path, relative_path)
    config.read(config_path)

    white_list_str = config['HOW_TO']['NAMING_RULE']
    white_list = [x.strip() for x in white_list_str.split(',')]

    return white_list


def load_initial_data(excel_file_path, excel_file_active_path):
    try:
        from_SP = pd.read_excel(excel_file_path)
        from_SP = from_SP.fillna('')
        from_SP = from_SP.astype(str)

        from_QA = pd.read_excel(excel_file_active_path)
        from_QA = from_QA.fillna('')
        from_QA = from_QA.astype(str)

        logger.info("Completed: Loading")
        return from_SP, from_QA
    except Exception as e:
        logger.error(traceback.format_exc())
        messagebox.showwarning("Warning", "Something was wrong.")
        sys.exit()


def extracting_original_df_using_diff(from_QA_original, diff_Q_only_in_QA):
    indexes = list(diff_Q_only_in_QA.index.values)
    ret = from_QA_original.iloc[indexes]
    return ret


def filtering_the_questions_only_in_qna(from_SP, from_QA, output_dir_path, intent):
    try:
        from_SP_copy = from_SP.copy()
        from_QA_copy = from_QA.copy()

        from_SP_clean, from_QA_clean = clean_and_unique_df(from_SP_copy, from_QA_copy, True)

        diff_Q_only_in_QA = from_QA_clean[
            ~from_QA_clean.QuestionAnswer.isin(from_SP_clean.QuestionAnswer)]

        indexes = list(diff_Q_only_in_QA.index.values)
        diff_Q_only_in_QA = from_QA.iloc[indexes]

        write_result(output_dir_path + '\\' + intent + '_diff_1_only_in_qa.xlsx', diff_Q_only_in_QA,
                     False)  # 01. filtering the questions only in QnA Maker.

        logger.info("Completed: " + output_dir_path + '\\' + '_diff_1_only_in_qa_from_qnamaker.xlsx')
        return diff_Q_only_in_QA
    except Exception as e:
        logger.error(traceback.format_exc())
        messagebox.showwarning("Warning", "Something was wrong.")
        sys.exit()


def filtering_if_suggested_questions_is_not_empty(from_QA, output_dir_path, intent):
    try:
        diff_suggested_only_in_QA = from_QA.loc[
            from_QA['SuggestedQuestions'] != '[]']  # 02. filtering if SuggestedQuestions is not empty.

        write_result(output_dir_path + '\\' + intent + '_diff_2_suggest.xlsx', diff_suggested_only_in_QA, False)

        logger.info("Completed: " + output_dir_path + '\\' + '_diff_2_suggest.xlsx')
        return diff_suggested_only_in_QA
    except Exception as e:
        logger.error(traceback.format_exc())
        messagebox.showwarning("Warning", "Something was wrong.")
        sys.exit()


def copy_result_input_hiragana(output_file_path, from_SP, tsv):
    base_path = os.path.abspath(".")
    save_path = os.path.join(base_path, 'input_hiragana')
    if not os.path.exists(save_path):
        os.mkdir(save_path)

    output_file_path = output_file_path.replace('ret_sync_active', 'input_hiragana')
    write_result(output_file_path, from_SP, tsv)


def clean_and_unique_df(from_SP_copy, from_QA_copy, mode):
    from_SP_copy['Question'] = from_SP_copy['Question'].apply(text_cleaner)
    from_QA_copy['Question'] = from_QA_copy['Question'].apply(text_cleaner)

    from_SP_copy['Answer'] = from_SP_copy['Answer'].apply(text_cleaner)
    from_QA_copy['Answer'] = from_QA_copy['Answer'].apply(text_cleaner)

    # ***** for Unique ID = Question + Answer ***** 
    if mode:
        from_SP_copy['QuestionAnswer'] = from_SP_copy['Question'] + from_SP_copy['Answer']
        from_QA_copy['QuestionAnswer'] = from_QA_copy['Question'] + from_QA_copy['Answer']

    return from_SP_copy, from_QA_copy


def compare_child_group_SP_and_QA(question, parent_childs_from_SP, parent_childs_from_QA):
    last_item_in_group = None

    for qa_group_name, child_group_from_QA in parent_childs_from_QA.groupby('QnaId'):
        for i, qa_row in child_group_from_QA.iterrows():
            if qa_row['Question'] == question:
                qa_parent = child_group_from_QA.head(1)
                qa_parent_question = qa_parent.iloc[0, 0]
                for sp_group_name, child_group_from_SP in parent_childs_from_SP.groupby('QnaId'):
                    for j, sp_qa_row in child_group_from_SP.iterrows():
                        sp_parent_question = sp_qa_row['Question']
                        if qa_parent_question == sp_parent_question:
                            last_item_in_group = child_group_from_SP.tail(1)
                            return last_item_in_group

    return last_item_in_group


def updating_SP_using_the_data_from_one_and_two(from_SP, from_QA, diff_Q_only_in_QA_copy, diff_suggested_only_in_QA,
                                                output_dir_path, intent):
    try:
        # 03. Updating SuggestedQuestions column of SharePoint
        from_SP_copy = from_SP.copy()
        from_QA_copy = from_QA.copy()
        from_SP_copy, from_QA_copy = clean_and_unique_df(from_SP_copy, from_QA_copy, True)

        for i, row in diff_suggested_only_in_QA.iterrows():
            question = row['Question']
            answer = row['Answer']
            suggestion_data = row['SuggestedQuestions']

            qa_clean_text = text_cleaner(question) + text_cleaner(answer)
            suggest_target_to_put = from_SP_copy.loc[from_SP_copy['QuestionAnswer'] == qa_clean_text]

            for j, suggest_item in suggest_target_to_put.iterrows():
                from_SP.loc[j, 'SuggestedQuestions'] = suggestion_data
                break

        logger.info("Completed: " + output_dir_path + '\\' + '_diff_3_update_suggest.xlsx')
        write_result(output_dir_path + '\\' + intent + '_diff_3_update_suggest.xlsx', from_SP, False)

        # 04. Adding Child questions from QnA Maker to SharePoint
        append_row = []
        for i, row in diff_Q_only_in_QA_copy.iterrows():
            question = row['Question']
            answer = row['Answer']

            answer_clean = text_cleaner(answer)
            parent_childs_from_SP = from_SP_copy.loc[from_SP_copy['Answer'] == answer_clean]
            parent_childs_from_QA = from_QA_copy.loc[from_QA_copy['Answer'] == answer_clean]

            question_clean = text_cleaner(question)
            last_item_in_group = compare_child_group_SP_and_QA(question_clean, parent_childs_from_SP, parent_childs_from_QA)

            if last_item_in_group is not None:
                group_tail_idx = last_item_in_group.index[0]
                row['QnaId'] = last_item_in_group['QnaId']
                index = group_tail_idx + 1
                from_SP = insert_row(index, from_SP, row)
                from_SP_copy = insert_row(index, from_SP_copy, row)
            else:
                append_row.append(row)

        # 05. adding the QA (QA Maker only) to last line
        if append_row:
            tail = from_SP.tail(1)
            last_qna_id = tail['QnaId']
            new_qna_id = int(float(last_qna_id)) + 1
            for row in append_row:
                row['QnaId'] = new_qna_id
                from_SP.loc[len(from_SP)] = row
                new_qna_id = new_qna_id + 1

        timestamp = time.strftime("%Y%m%d-%H%M%S")
        filename, file_extension = os.path.splitext(output_dir_path + '\\' + intent + '_diff_final.xlsx')
        output_file_path = filename + '_' + timestamp + file_extension

        logger.info("Completed: " + output_file_path)
        print("Completed: " + output_file_path)
        write_result(output_file_path, from_SP, True)

        messagebox.showinfo("Information", "Completed: Sync")
    except Exception as e:
        logger.error(traceback.format_exc())
        messagebox.showwarning("Warning", "Something was wrong.")
        sys.exit()


if __name__ == "__main__":
    base_path = os.path.abspath(".")
    output_path = os.path.join(base_path, 'ret_sync_active')
    if not os.path.exists(output_path):
        os.mkdir(output_path)

    output_dir_path = output_path
    excel_file_path = ''
    excel_file_active_path = ''

    excel_path = filedialog.askopenfile(mode='r', filetypes=[('Prev Train Data (Excel) Files', '*.xlsx')])
    if excel_path:
        excel_file_path = str(excel_path.name)
        print(excel_file_path)
    else:
        messagebox.showinfo("Information", "Please set the file.")

    excel_path = filedialog.askopenfile(mode='r', filetypes=[('QnA Maker (Excel) Files', '*.xlsx')])
    if excel_path:
        excel_file_active_path = str(excel_path.name)
        print(excel_file_active_path)
    else:
        messagebox.showinfo("Information", "Please set the file.")

    white_list = white_list_gen()
    intent = remove_prefix_suffix(excel_file_active_path, white_list)

    from_SP, from_QA = load_initial_data(excel_file_path, excel_file_active_path)

    # 01. filtering the questions only in QnA Maker
    diff_Q_only_in_QA_copy = filtering_the_questions_only_in_qna(from_SP, from_QA, output_dir_path, intent)

    # 02. filtering if SuggestedQuestions is not empty.
    diff_suggested_only_in_QA = filtering_if_suggested_questions_is_not_empty(from_QA, output_dir_path, intent)

    # 03. updation SP using 01 and 02 data
    updating_SP_using_the_data_from_one_and_two(from_SP, from_QA, diff_Q_only_in_QA_copy, diff_suggested_only_in_QA,
                                                output_dir_path, intent)
