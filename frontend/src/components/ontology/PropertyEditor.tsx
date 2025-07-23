import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Checkbox,
  FormControlLabel,
  Grid,
  Tabs,
  Tab,
  Box,
  Typography,
  Chip,
  Alert,
  FormHelperText,
} from '@mui/material';
import { Property } from '../../types/ontology';

interface PropertyEditorProps {
  open: boolean;
  onClose: () => void;
  property?: Property;
  onSave: (property: Property) => void;
}

const dataTypes = [
  { value: 'xsd:string', label: 'String' },
  { value: 'xsd:integer', label: 'Integer' },
  { value: 'xsd:decimal', label: 'Decimal' },
  { value: 'xsd:boolean', label: 'Boolean' },
  { value: 'xsd:date', label: 'Date' },
  { value: 'xsd:dateTime', label: 'DateTime' },
  { value: 'array', label: 'Array' },
  { value: 'link', label: 'Link (Relationship)' },
];

export const PropertyEditor: React.FC<PropertyEditorProps> = ({
  open,
  onClose,
  property,
  onSave,
}) => {
  const [tabValue, setTabValue] = useState(0);
  const [formData, setFormData] = useState<Property>(
    property || {
      name: '',
      type: 'xsd:string',
      label: '',
      required: false,
      description: '',
    }
  );
  const [errors, setErrors] = useState<Record<string, string>>({});

  const handleChange = (field: keyof Property) => (
    event: React.ChangeEvent<HTMLInputElement | { value: unknown }>
  ) => {
    setFormData({
      ...formData,
      [field]: event.target.value,
    });
    // Clear error when user starts typing
    if (errors[field]) {
      setErrors({ ...errors, [field]: '' });
    }
  };

  const handleCheckboxChange = (field: keyof Property) => (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    setFormData({
      ...formData,
      [field]: event.target.checked,
    });
  };

  const validate = (): boolean => {
    const newErrors: Record<string, string> = {};

    if (!formData.name) {
      newErrors.name = '속성 이름은 필수입니다';
    } else if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(formData.name)) {
      newErrors.name = '속성 이름은 영문자로 시작하고 영문자, 숫자, 밑줄만 포함할 수 있습니다';
    }

    if (!formData.label) {
      newErrors.label = '표시 이름은 필수입니다';
    }

    if (!formData.type) {
      newErrors.type = '데이터 타입은 필수입니다';
    }

    if (formData.type === 'link' && !formData.target && !formData.linkTarget) {
      newErrors.target = '링크 타입의 경우 대상 클래스를 지정해야 합니다';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSave = () => {
    if (validate()) {
      onSave(formData);
      onClose();
    }
  };

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>
        {property ? '속성 편집' : '새 속성 만들기'}
      </DialogTitle>
      
      <DialogContent>
        <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
          <Tabs value={tabValue} onChange={handleTabChange}>
            <Tab label="일반" />
            <Tab label="표시" />
            <Tab label="검증" />
          </Tabs>
        </Box>

        {tabValue === 0 && (
          <Box sx={{ pt: 2 }}>
            <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 2 }}>
              <Box>
                <TextField
                  fullWidth
                  label="속성 이름 (API Name)"
                  value={formData.name}
                  onChange={handleChange('name')}
                  error={!!errors.name}
                  helperText={errors.name}
                  disabled={!!property} // 기존 속성은 이름 변경 불가
                />
              </Box>
              
              <Box>
                <TextField
                  fullWidth
                  label="표시 이름"
                  value={formData.label}
                  onChange={handleChange('label')}
                  error={!!errors.label}
                  helperText={errors.label}
                />
              </Box>

              <Box sx={{ gridColumn: '1 / -1' }}>
                <TextField
                  fullWidth
                  label="설명"
                  value={formData.description || ''}
                  onChange={handleChange('description')}
                  multiline
                  rows={2}
                />
              </Box>

              <Box>
                <FormControl fullWidth error={!!errors.type}>
                  <InputLabel>데이터 타입</InputLabel>
                  <Select
                    value={formData.type}
                    onChange={handleChange('type') as any}
                    label="데이터 타입"
                  >
                    {dataTypes.map((type) => (
                      <MenuItem key={type.value} value={type.value}>
                        {type.label}
                      </MenuItem>
                    ))}
                  </Select>
                  {errors.type && <FormHelperText>{errors.type}</FormHelperText>}
                </FormControl>
              </Box>

              {formData.type === 'link' && (
                <Box>
                  <TextField
                    fullWidth
                    label="대상 클래스"
                    value={formData.target || formData.linkTarget || ''}
                    onChange={(e) => {
                      setFormData({
                        ...formData,
                        target: e.target.value,
                        linkTarget: e.target.value,
                      });
                    }}
                    error={!!errors.target}
                    helperText={errors.target || '연결할 대상 클래스를 입력하세요'}
                  />
                </Box>
              )}

              {formData.type === 'link' && (
                <Box>
                  <FormControl fullWidth>
                    <InputLabel>Cardinality</InputLabel>
                    <Select
                      value={formData.cardinality || '1:n'}
                      onChange={handleChange('cardinality') as any}
                      label="Cardinality"
                    >
                      <MenuItem value="1:1">1:1 (One to One)</MenuItem>
                      <MenuItem value="1:n">1:n (One to Many)</MenuItem>
                      <MenuItem value="n:1">n:1 (Many to One)</MenuItem>
                      <MenuItem value="n:m">n:m (Many to Many)</MenuItem>
                    </Select>
                  </FormControl>
                </Box>
              )}

              <Box sx={{ gridColumn: '1 / -1' }}>
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={formData.required}
                      onChange={handleCheckboxChange('required')}
                    />
                  }
                  label="필수 속성"
                />
              </Box>

              <Box sx={{ gridColumn: '1 / -1' }}>
                <Alert severity="info" sx={{ mt: 1 }}>
                  <Typography variant="body2">
                    상태: <Chip label="Active" size="small" color="success" />
                  </Typography>
                  <Typography variant="caption">
                    Active 상태의 속성은 API 이름을 변경할 수 없습니다.
                  </Typography>
                </Alert>
              </Box>
            </Box>
          </Box>
        )}

        {tabValue === 1 && (
          <Box sx={{ pt: 2 }}>
            <Typography variant="body2" color="text.secondary">
              표시 형식 및 조건부 서식 설정이 여기에 표시됩니다.
            </Typography>
          </Box>
        )}

        {tabValue === 2 && (
          <Box sx={{ pt: 2 }}>
            <Typography variant="body2" color="text.secondary">
              값 검증 규칙 및 제약 조건 설정이 여기에 표시됩니다.
            </Typography>
          </Box>
        )}
      </DialogContent>

      <DialogActions>
        <Button onClick={onClose}>취소</Button>
        <Button onClick={handleSave} variant="contained">
          저장
        </Button>
      </DialogActions>
    </Dialog>
  );
};